/***************************************************************************
 * Export scanned record batches through the Arrow C data interface.
 *
 * Arrays point straight at a batch's column arrays and records, so nothing is
 * copied. Every exported array (the struct, its children and the source ID
 * dictionary) holds a reference to state shared by the export, since a
 * consumer may move children out and release them separately. Releasing the
 * last one releases the export's owner, which keeps the batch alive.
 ***************************************************************************/

#include <Python.h>

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "libmseed.h"
#include "pymseed_arrow.h"

#if defined(_MSC_VER)
#include <windows.h>
#define PM_ATOMIC_DECREMENT(p) InterlockedDecrement ((volatile LONG *)(p))
#define PM_ATOMIC_INCREMENT(p) InterlockedIncrement ((volatile LONG *)(p))
typedef LONG pm_refcount_t;
#else
#define PM_ATOMIC_DECREMENT(p) __atomic_sub_fetch ((p), 1, __ATOMIC_ACQ_REL)
#define PM_ATOMIC_INCREMENT(p) __atomic_add_fetch ((p), 1, __ATOMIC_RELAXED)
typedef int32_t pm_refcount_t;
#endif

/* Run `call` holding the GIL, with any Python exception already pending (e.g.
   a consumer releasing an array or stream while raising) set aside, since the
   Python callbacks can't run with one set. Skipped once Python has shut down,
   leaving what the callback would release to the process exit. */
#define PM_CALL_PYTHON(call)                         \
  do                                                 \
  {                                                  \
    if (Py_IsInitialized ())                         \
    {                                                \
      PyGILState_STATE gil = PyGILState_Ensure ();   \
      PyObject *type, *value, *traceback;            \
      PyErr_Fetch (&type, &value, &traceback);       \
      call;                                          \
      PyErr_Restore (type, value, traceback);        \
      PyGILState_Release (gil);                      \
    }                                                \
  } while (0)

/* Fields of an exported batch, in order */
static const struct
{
  const char *name;
  const char *format;
} fields[] = {
    {"sourceid", "i"}, /* dictionary-encoded, see SOURCEID_VALUES_FORMAT */
    {"starttime", "tsn:UTC"},
    {"endtime", "tsn:UTC"},
    {"samprate", "g"},
    {"samplecnt", "l"},
    {"encoding", "s"},
    {"pubversion", "C"},
    {"formatversion", "C"},
    {"record", "z"},
};
#define NUM_FIELDS ((int64_t)(sizeof (fields) / sizeof (fields[0])))
#define SOURCEID_VALUES_FORMAT "u"
#define FIELD_SOURCEID 0
#define FIELD_RECORD (NUM_FIELDS - 1)
#define FIELD_BIT(i) ((uint32_t)1 << (i))

static int64_t
count_fields (uint32_t field_mask)
{
  int64_t count = 0;
  int64_t i;

  for (i = 0; i < NUM_FIELDS; i++)
    count += (field_mask & FIELD_BIT (i)) != 0;
  return count;
}

uint32_t
pm_columns_field_mask (const PMRecordColumns *cols)
{
  const void *columns[] = {
      cols->sid_index, cols->starttime,  cols->endtime,       cols->samprate, cols->samplecnt,
      cols->encoding,  cols->pubversion, cols->formatversion, cols->offset,
  };
  uint32_t field_mask = 0;
  int64_t i;

  for (i = 0; i < NUM_FIELDS; i++)
    if (columns[i])
      field_mask |= FIELD_BIT (i);
  return field_mask;
}

/***************************************************************************
 * Schema
 ***************************************************************************/

static void
release_schema (struct ArrowSchema *schema)
{
  int64_t i;

  for (i = 0; i < schema->n_children; i++)
  {
    if (schema->children[i]->release)
      schema->children[i]->release (schema->children[i]);
    free (schema->children[i]);
  }
  free (schema->children);

  if (schema->dictionary)
  {
    if (schema->dictionary->release)
      schema->dictionary->release (schema->dictionary);
    free (schema->dictionary);
  }

  schema->release = NULL;
}

/* Fill `schema` with a non-nullable field and room for `n_children` children */
static int
init_schema (struct ArrowSchema *schema, const char *format, const char *name, int64_t n_children)
{
  memset (schema, 0, sizeof (*schema));
  schema->format  = format;
  schema->name    = name;
  schema->release = release_schema;

  if (n_children > 0)
  {
    schema->children = calloc ((size_t)n_children, sizeof (struct ArrowSchema *));
    if (!schema->children)
      return -1;
    schema->n_children = n_children;
  }
  return 0;
}

int
pm_arrow_export_schema (struct ArrowSchema *out, uint32_t field_mask)
{
  struct ArrowSchema *child;
  int64_t i;

  if (init_schema (out, "+s", "", count_fields (field_mask)))
    return -1;
  out->n_children = 0; /* Counts the children built, so a failure frees only those */

  for (i = 0; i < NUM_FIELDS; i++)
  {
    if (!(field_mask & FIELD_BIT (i)))
      continue;

    if (!(child = calloc (1, sizeof (*child))))
    {
      release_schema (out);
      return -1;
    }
    out->children[out->n_children++] = child;
    if (init_schema (child, fields[i].format, fields[i].name, 0))
    {
      release_schema (out);
      return -1;
    }

    /* Source IDs are int32 indices into a dictionary of strings */
    if (i == FIELD_SOURCEID &&
        (!(child->dictionary = calloc (1, sizeof (*child))) ||
         init_schema (child->dictionary, SOURCEID_VALUES_FORMAT, "", 0)))
    {
      release_schema (out);
      return -1;
    }
  }

  return 0;
}

/***************************************************************************
 * Array
 ***************************************************************************/

/* State shared by an export's arrays */
typedef struct PMExport
{
  pm_refcount_t refcount;
  void *owner;
  int32_t *sid_offsets; /* Source ID dictionary: string offsets */
  char *sid_data;       /* Source ID dictionary: string bytes */
} PMExport;

static void
unref_export (PMExport *export)
{
  if (PM_ATOMIC_DECREMENT (&export->refcount) == 0)
  {
    free (export->sid_offsets);
    free (export->sid_data);
    PM_CALL_PYTHON (pm_arrow_release_owner (export->owner));
    free (export);
  }
}

static void
release_array (struct ArrowArray *array)
{
  int64_t i;

  for (i = 0; i < array->n_children; i++)
  {
    if (array->children[i]->release)
      array->children[i]->release (array->children[i]);
    free (array->children[i]);
  }
  free (array->children);

  if (array->dictionary)
  {
    if (array->dictionary->release)
      array->dictionary->release (array->dictionary);
    free (array->dictionary);
  }

  free (array->buffers);
  unref_export ((PMExport *)array->private_data);
  array->release = NULL;
}

/* Allocate an array of `length` elements referencing `export`, with no nulls */
static struct ArrowArray *
new_array (PMExport *export, int64_t length, int64_t n_buffers, int64_t n_children)
{
  struct ArrowArray *array = calloc (1, sizeof (*array));

  if (!array)
    return NULL;

  array->buffers  = calloc ((size_t)n_buffers, sizeof (void *));
  array->children = n_children ? calloc ((size_t)n_children, sizeof (struct ArrowArray *)) : NULL;
  if (!array->buffers || (n_children && !array->children))
  {
    free (array->buffers);
    free (array->children);
    free (array);
    return NULL;
  }

  array->length       = length;
  array->n_buffers    = n_buffers;
  array->n_children   = n_children;
  array->release      = release_array;
  array->private_data = export;
  PM_ATOMIC_INCREMENT (&export->refcount);
  return array;
}

/* The source IDs as an Arrow string array */
static struct ArrowArray *
sourceid_dictionary (PMExport *export, const PMRecordColumns *cols)
{
  struct ArrowArray *array;
  size_t length;
  int32_t total = 0;
  int32_t i;

  export->sid_offsets = malloc (((size_t)cols->nsids + 1) * sizeof (int32_t));
  export->sid_data    = malloc ((size_t)cols->nsids * LM_SIDLEN + 1);
  if (!export->sid_offsets || !export->sid_data)
    return NULL;

  export->sid_offsets[0] = 0;
  for (i = 0; i < cols->nsids; i++)
  {
    length = strnlen (cols->sids + (int64_t)i * LM_SIDLEN, LM_SIDLEN);
    memcpy (export->sid_data + total, cols->sids + (int64_t)i * LM_SIDLEN, length);
    total += (int32_t)length;
    export->sid_offsets[i + 1] = total;
  }

  if (!(array = new_array (export, cols->nsids, 3, 0)))
    return NULL;
  array->buffers[1] = export->sid_offsets;
  array->buffers[2] = export->sid_data;
  return array;
}

int
pm_arrow_export_array (const PMRecordColumns *cols, const char *records, void *owner,
                       struct ArrowArray *out)
{
  const void *data[] = {
      cols->sid_index, cols->starttime,  cols->endtime,       cols->samprate, cols->samplecnt,
      cols->encoding,  cols->pubversion, cols->formatversion, cols->offset,
  };
  uint32_t field_mask = pm_columns_field_mask (cols);
  PMExport *export;
  struct ArrowArray *parent;
  struct ArrowArray *child;
  int64_t n = cols->count;
  int64_t i;

  if (!(export = calloc (1, sizeof (*export))))
  {
    PM_CALL_PYTHON (pm_arrow_release_owner (owner));
    return -1;
  }
  /* Held until the parent is built, so a failure partway releases everything once */
  export->refcount = 1;
  export->owner    = owner;

  if (!(parent = new_array (export, n, 1, count_fields (field_mask))))
    goto fail;
  parent->n_children = 0; /* Counts the children built, so a failure frees only those */

  for (i = 0; i < NUM_FIELDS; i++)
  {
    if (!(field_mask & FIELD_BIT (i)))
      continue;

    /* Records are variable-length binary: offsets, then data */
    if (!(child = new_array (export, n, i == FIELD_RECORD ? 3 : 2, 0)))
      goto fail;
    parent->children[parent->n_children++] = child;
    child->buffers[1] = data[i];
    if (i == FIELD_RECORD)
      child->buffers[2] = records;
    if (i == FIELD_SOURCEID && !(child->dictionary = sourceid_dictionary (export, cols)))
      goto fail;
  }

  *out = *parent;
  free (parent);
  unref_export (export);
  return 0;

fail:
  if (parent)
  {
    release_array (parent);
    free (parent);
  }
  unref_export (export);
  return -1;
}

/***************************************************************************
 * Stream
 ***************************************************************************/

typedef struct PMStream
{
  void *source;
  uint32_t field_mask;
  char error[PM_ARROW_ERROR_LEN];
} PMStream;

static int
stream_get_schema (struct ArrowArrayStream *stream, struct ArrowSchema *out)
{
  PMStream *private = (PMStream *)stream->private_data;

  if (pm_arrow_export_schema (out, private->field_mask))
  {
    strcpy (private->error, "Out of memory exporting miniSEED record batch schema");
    return ENOMEM;
  }
  return 0;
}

static int
stream_get_next (struct ArrowArrayStream *stream, struct ArrowArray *out)
{
  PMStream *private = (PMStream *)stream->private_data;

  int status = EIO;

  /* Replaced unless Python has shut down */
  strcpy (private->error, "Python has shut down");
  PM_CALL_PYTHON (private->error[0] = '\0';
                  status = pm_arrow_stream_next (private->source, out, private->error));
  return status;
}

static const char *
stream_get_last_error (struct ArrowArrayStream *stream)
{
  PMStream *private = (PMStream *)stream->private_data;

  return private->error[0] ? private->error : NULL;
}

static void
stream_release (struct ArrowArrayStream *stream)
{
  PMStream *private = (PMStream *)stream->private_data;

  PM_CALL_PYTHON (pm_arrow_stream_release (private->source));
  free (private);
  stream->release = NULL;
}

struct ArrowArrayStream *
pm_arrow_new_stream (void *source, uint32_t field_mask)
{
  struct ArrowArrayStream *stream = calloc (1, sizeof (*stream));
  PMStream *private = calloc (1, sizeof (*private));

  if (!stream || !private)
  {
    free (stream);
    free (private);
    PM_CALL_PYTHON (pm_arrow_stream_release (source));
    return NULL;
  }

  private->source        = source;
  private->field_mask    = field_mask;
  stream->get_schema     = stream_get_schema;
  stream->get_next       = stream_get_next;
  stream->get_last_error = stream_get_last_error;
  stream->release        = stream_release;
  stream->private_data   = private;
  return stream;
}

/***************************************************************************
 * PyCapsules
 ***************************************************************************/

struct ArrowSchema *
pm_arrow_new_schema (uint32_t field_mask)
{
  struct ArrowSchema *schema = malloc (sizeof (*schema));

  if (schema && pm_arrow_export_schema (schema, field_mask))
  {
    free (schema);
    return NULL;
  }
  return schema;
}

struct ArrowArray *
pm_arrow_new_array (const PMRecordColumns *cols, const char *records, void *owner)
{
  struct ArrowArray *array = malloc (sizeof (*array));

  if (!array)
  {
    PM_CALL_PYTHON (pm_arrow_release_owner (owner));
    return NULL;
  }
  if (pm_arrow_export_array (cols, records, owner, array))
  {
    free (array);
    return NULL;
  }
  return array;
}

void
pm_arrow_schema_capsule_destructor (void *capsule)
{
  struct ArrowSchema *schema = PyCapsule_GetPointer ((PyObject *)capsule, "arrow_schema");

  if (schema)
  {
    if (schema->release)
      schema->release (schema);
    free (schema);
  }
  else
  {
    PyErr_Clear ();
  }
}

void
pm_arrow_array_capsule_destructor (void *capsule)
{
  struct ArrowArray *array = PyCapsule_GetPointer ((PyObject *)capsule, "arrow_array");

  if (array)
  {
    if (array->release)
      array->release (array);
    free (array);
  }
  else
  {
    PyErr_Clear ();
  }
}

void
pm_arrow_stream_capsule_destructor (void *capsule)
{
  struct ArrowArrayStream *stream =
      PyCapsule_GetPointer ((PyObject *)capsule, "arrow_array_stream");

  if (stream)
  {
    if (stream->release)
      stream->release (stream);
    free (stream);
  }
  else
  {
    PyErr_Clear ();
  }
}

#ifndef PYMSEED_ARROW_H
#define PYMSEED_ARROW_H

#include <stdint.h>

#include "pymseed_scan.h"

/* Arrow C data interface, see https://arrow.apache.org/docs/format/CDataInterface.html */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema
{
  const char *format;
  const char *name;
  const char *metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema **children;
  struct ArrowSchema *dictionary;
  void (*release) (struct ArrowSchema *);
  void *private_data;
};

struct ArrowArray
{
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void **buffers;
  struct ArrowArray **children;
  struct ArrowArray *dictionary;
  void (*release) (struct ArrowArray *);
  void *private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream
{
  int (*get_schema) (struct ArrowArrayStream *, struct ArrowSchema *out);
  int (*get_next) (struct ArrowArrayStream *, struct ArrowArray *out);
  const char *(*get_last_error) (struct ArrowArrayStream *);
  void (*release) (struct ArrowArrayStream *);
  void *private_data;
};

#endif /* ARROW_C_STREAM_INTERFACE */

#define PM_ARROW_ERROR_LEN 1024

/* The fields an export of `cols` holds, one bit per column allocated, in field
   order: sourceid, starttime, endtime, samprate, samplecnt, encoding,
   pubversion, formatversion, record. */
extern uint32_t pm_columns_field_mask (const PMRecordColumns *cols);

/* Fill `out` with the schema of an exported batch: a struct with a field per
   column in `field_mask`. */
extern int pm_arrow_export_schema (struct ArrowSchema *out, uint32_t field_mask);

/* Fill `out` with a struct array over a scanned batch's columns, without copying
   them: a field per column allocated. `records` is where the batch's records
   start (the buffer plus `cols->base`), if `cols->offset` is allocated.

   The columns and records must stay valid until every array exported from them
   is released. `owner` identifies what keeps them valid: once the last is
   released, pm_arrow_release_owner(owner) is called, from whatever thread
   released it; it's also called if the export fails. Returns 0, or -1 if
   memory runs out. */
extern int pm_arrow_export_array (const PMRecordColumns *cols, const char *records, void *owner,
                                  struct ArrowArray *out);

/* As pm_arrow_export_schema() and pm_arrow_export_array(), into structs they
   allocate for a PyCapsule to own. Return NULL if memory runs out. */
extern struct ArrowSchema *pm_arrow_new_schema (uint32_t field_mask);
extern struct ArrowArray *pm_arrow_new_array (const PMRecordColumns *cols, const char *records,
                                              void *owner);

/* A stream of batches with the fields in `field_mask`, pulled from `source`
   with pm_arrow_stream_next(), in a
   struct allocated for a PyCapsule to own. pm_arrow_stream_release(source) is
   called when the stream is released, or if this fails. Returns NULL if memory
   runs out. */
extern struct ArrowArrayStream *pm_arrow_new_stream (void *source, uint32_t field_mask);

/* Implemented in Python: release what kept an export's columns valid */
extern void pm_arrow_release_owner (void *owner);

/* Implemented in Python: export the next batch from `source` into `out`, or set
   `out->release` to NULL at the end. On error, write a message of at most
   PM_ARROW_ERROR_LEN bytes (NUL included) to `error` and return an errno code. */
extern int pm_arrow_stream_next (void *source, struct ArrowArray *out, char *error);

/* Implemented in Python: release a stream's source */
extern void pm_arrow_stream_release (void *source);

/* PyCapsule destructors for capsules named "arrow_schema", "arrow_array" and
   "arrow_array_stream", taking the capsule as a PyObject *. */
extern void pm_arrow_schema_capsule_destructor (void *capsule);
extern void pm_arrow_array_capsule_destructor (void *capsule);
extern void pm_arrow_stream_capsule_destructor (void *capsule);

#endif /* PYMSEED_ARROW_H */

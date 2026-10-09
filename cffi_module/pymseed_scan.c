/***************************************************************************
 * Scan a buffer of miniSEED records into per-field column arrays.
 *
 * Parsing record by record from Python costs far more than parsing itself:
 * every record becomes a Python object and every field a call. This loops
 * over the records in C and fills arrays the caller provides, one element
 * per record, so Python (or Arrow) can take each field as a whole column.
 ***************************************************************************/

#include <stdint.h>
#include <string.h>

#include "libmseed.h"
#include "pymseed_scan.h"

/* FNV-1a hash of a NUL-terminated source ID */
static uint32_t
hash_sid (const char *sid)
{
  uint32_t hash = 2166136261u;
  int i;

  for (i = 0; i < LM_SIDLEN && sid[i]; i++)
  {
    hash ^= (uint8_t)sid[i];
    hash *= 16777619u;
  }
  return hash;
}

/* Find `sid` in the batch's source ID table, adding it if it's new.
 * Returns its index, or -1 if the table is full (it can't be: a batch has no
 * more source IDs than records). */
static int32_t
sid_index (PMRecordColumns *cols, const char *sid, int32_t *last)
{
  uint32_t hash;
  uint32_t slot;
  int32_t idx;
  char *entry;

  /* Consecutive records are usually from the same source */
  if (*last >= 0 && strncmp (cols->sids + (int64_t)*last * LM_SIDLEN, sid, LM_SIDLEN) == 0)
    return *last;

  /* Open addressing with linear probing; the table is at most half full */
  hash = hash_sid (sid);
  for (slot = hash & (uint32_t)cols->sid_slot_mask; (idx = cols->sid_slots[slot]) >= 0;
       slot = (slot + 1) & (uint32_t)cols->sid_slot_mask)
  {
    if (cols->sid_hashes[idx] == hash &&
        strncmp (cols->sids + (int64_t)idx * LM_SIDLEN, sid, LM_SIDLEN) == 0)
      return *last = idx;
  }

  if (cols->nsids >= cols->capacity)
    return -1;

  idx   = cols->nsids++;
  entry = cols->sids + (int64_t)idx * LM_SIDLEN;
  strncpy (entry, sid, LM_SIDLEN - 1);
  entry[LM_SIDLEN - 1]   = '\0';
  cols->sid_hashes[idx]  = hash;
  cols->sid_slots[slot]  = idx;
  return *last = idx;
}

int
pm_scan_records (const char *buffer, uint64_t buflen, uint32_t flags, int8_t verbose,
                 PMRecordColumns *cols, uint64_t *consumed)
{
  MS3Record *msr = NULL;
  uint64_t offset = *consumed;
  int64_t n = 0;
  int32_t last_sid = -1;
  int32_t sid;
  int status;
  int result = PM_SCAN_NEEDMORE;

  cols->count = 0;
  cols->nsids = 0;
  cols->base  = (int64_t)offset;
  if (cols->sid_index)
    memset (cols->sid_slots, 0xFF, ((size_t)cols->sid_slot_mask + 1) * sizeof (int32_t)); /* all -1 */
  if (cols->offset)
    cols->offset[0] = 0;

  while (buflen - offset >= MINRECLEN)
  {
    if (n >= cols->capacity)
    {
      result = PM_SCAN_FULL;
      break;
    }

    status = msr3_parse (buffer + offset, buflen - offset, &msr, flags, verbose);

    if (status > 0) /* More data needed to finish this record */
      break;

    if (status < 0)
    {
      result = status;
      break;
    }

    /* Keep record offsets relative to the batch within int32 range, as Arrow's
       binary type requires */
    if (cols->offset && (int64_t)(offset - (uint64_t)cols->base) + msr->reclen > INT32_MAX)
    {
      result = PM_SCAN_FULL;
      break;
    }

    /* Fill only the columns requested, i.e. allocated */
    if (cols->sid_index)
    {
      if ((sid = sid_index (cols, msr->sid, &last_sid)) < 0)
      {
        result = PM_SCAN_FULL;
        break;
      }
      cols->sid_index[n] = sid;
    }
    if (cols->starttime)
      cols->starttime[n] = msr->starttime;
    if (cols->endtime)
      cols->endtime[n] = msr3_endtime (msr);
    if (cols->samprate)
      cols->samprate[n] = msr3_sampratehz (msr);
    if (cols->samplecnt)
      cols->samplecnt[n] = msr->samplecnt;
    if (cols->encoding)
      cols->encoding[n] = msr->encoding;
    if (cols->pubversion)
      cols->pubversion[n] = msr->pubversion;
    if (cols->formatversion)
      cols->formatversion[n] = msr->formatversion;

    offset += (uint64_t)msr->reclen;
    n++;
    if (cols->offset)
      cols->offset[n] = (int32_t)(offset - (uint64_t)cols->base);
  }

  msr3_free (&msr);

  cols->count = n;
  *consumed   = offset;
  return result;
}

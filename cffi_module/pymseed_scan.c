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

/* Find `sid` in the batch's source ID table, adding it if there's room.
 * Returns its index, or -1 if the table is full. */
static int32_t
sid_index (PMRecordColumns *cols, const char *sid, int32_t *last)
{
  char *entry;
  int32_t idx;

  /* Consecutive records are usually from the same source */
  if (*last >= 0 && strncmp (cols->sids + (int64_t)*last * LM_SIDLEN, sid, LM_SIDLEN) == 0)
    return *last;

  for (idx = 0; idx < cols->nsids; idx++)
  {
    if (strncmp (cols->sids + (int64_t)idx * LM_SIDLEN, sid, LM_SIDLEN) == 0)
      return *last = idx;
  }

  if (cols->nsids >= cols->sid_capacity)
    return -1;

  entry = cols->sids + (int64_t)cols->nsids * LM_SIDLEN;
  strncpy (entry, sid, LM_SIDLEN - 1);
  entry[LM_SIDLEN - 1] = '\0';
  return *last = cols->nsids++;
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
  cols->base = (int64_t)offset;
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
    if ((int64_t)(offset - (uint64_t)cols->base) + msr->reclen > INT32_MAX)
    {
      result = PM_SCAN_FULL;
      break;
    }

    if ((sid = sid_index (cols, msr->sid, &last_sid)) < 0)
    {
      result = PM_SCAN_FULL;
      break;
    }

    cols->sid_index[n]     = sid;
    cols->starttime[n]     = msr->starttime;
    cols->endtime[n]       = msr3_endtime (msr);
    cols->samprate[n]      = msr3_sampratehz (msr);
    cols->samplecnt[n]     = msr->samplecnt;
    cols->encoding[n]      = msr->encoding;
    cols->pubversion[n]    = msr->pubversion;
    cols->formatversion[n] = msr->formatversion;

    offset += (uint64_t)msr->reclen;
    n++;
    cols->offset[n] = (int32_t)(offset - (uint64_t)cols->base);
  }

  msr3_free (&msr);

  cols->count = n;
  *consumed   = offset;
  return result;
}

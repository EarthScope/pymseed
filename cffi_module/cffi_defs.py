"""
CFFI definitions for pymseed - contains functions and definitions used by the package
"""

LIBRARY_CDEF = """

// Core types
typedef int64_t nstime_t;

// Version information (as extern symbols)
extern const char LIBMSEED_VERSION[];
extern const char LIBMSEED_RELEASE[];

// Essential constants
#define LM_SIDLEN 64
#define NSTMODULUS 1000000000
#define NSTERROR -2145916800000000000LL
#define NSTUNSET -2145916799999999999LL
#define MINRECLEN 40
#define MAXRECLEN 10485760
#define MAXRECLENv2 131172
#define MSTRACEID_SKIPLIST_HEIGHT 8

// Error codes
#define MS_ENDOFFILE        1
#define MS_NOERROR          0
#define MS_GENERROR        -1
#define MS_NOTSEED         -2
#define MS_WRONGLENGTH     -3
#define MS_OUTOFRANGE      -4
#define MS_UNKNOWNFORMAT   -5
#define MS_STBADCOMPFLAG   -6
#define MS_INVALIDCRC      -7

// Byte swap flags
#define MSSWAP_HEADER   0x01
#define MSSWAP_PAYLOAD  0x02

// Control flags
#define MSF_UNPACKDATA    0x0001
#define MSF_SKIPNOTDATA   0x0002
#define MSF_VALIDATECRC   0x0004
#define MSF_PNAMERANGE    0x0008
#define MSF_ATENDOFFILE   0x0010
#define MSF_SEQUENCE      0x0020
#define MSF_FLUSHDATA     0x0040
#define MSF_PACKVER2      0x0080
#define MSF_RECORDLIST    0x0100
#define MSF_MAINTAINMSTL  0x0200
#define MSF_PPUPDATETIME  0x0400

// Data encodings
#define DE_TEXT        0
#define DE_INT16       1
#define DE_INT32       3
#define DE_FLOAT32     4
#define DE_FLOAT64     5
#define DE_STEIM1      10
#define DE_STEIM2      11
#define DE_GEOSCOPE24  12
#define DE_GEOSCOPE163 13
#define DE_GEOSCOPE164 14
#define DE_CDSN        16
#define DE_SRO         30
#define DE_DWWSSN      32

// Time format enumeration
typedef enum
{
  ISOMONTHDAY         = 0,
  ISOMONTHDAY_Z       = 1,
  ISOMONTHDAY_DOY     = 2,
  ISOMONTHDAY_DOY_Z   = 3,
  ISOMONTHDAY_SPACE   = 4,
  ISOMONTHDAY_SPACE_Z = 5,
  SEEDORDINAL         = 6,
  UNIXEPOCH           = 7,
  NANOSECONDEPOCH     = 8
} ms_timeformat_t;

// Subseconds format enumeration
typedef enum
{
  NONE            = 0,
  MICRO           = 1,
  NANO            = 2,
  MICRO_NONE      = 3,
  NANO_NONE       = 4,
  NANO_MICRO      = 5,
  NANO_MICRO_NONE = 6
} ms_subseconds_t;

// Core structures
typedef struct MS3Record {
  const char     *record;
  int32_t         reclen;
  uint8_t         swapflag;
  char            sid[LM_SIDLEN];
  uint8_t         formatversion;
  uint8_t         flags;
  nstime_t        starttime;
  double          samprate;
  int16_t         encoding;
  uint8_t         pubversion;
  int64_t         samplecnt;
  uint32_t        crc;
  uint16_t        extralength;
  uint32_t        datalength;
  char           *extra;
  void           *datasamples;
  uint64_t        datasize;
  int64_t         numsamples;
  char            sampletype;
} MS3Record;

typedef struct MS3SelectTime {
  nstime_t starttime;
  nstime_t endtime;
  struct MS3SelectTime *next;
} MS3SelectTime;

typedef struct MS3Selections {
  char sidpattern[100];
  struct MS3SelectTime *timewindows;
  struct MS3Selections *next;
  uint8_t pubversion;
} MS3Selections;

typedef struct MS3RecordPtr
{
  const char *bufferptr;
  FILE *fileptr;
  const char *filename;
  int64_t fileoffset;
  MS3Record *msr;
  nstime_t endtime;
  uint32_t dataoffset;
  void *prvtptr;
  struct MS3RecordPtr *next;
} MS3RecordPtr;

typedef struct MS3RecordList
{
  uint64_t recordcnt;
  MS3RecordPtr *first;
  MS3RecordPtr *last;
} MS3RecordList;

typedef struct MS3TraceSeg {
  nstime_t        starttime;
  nstime_t        endtime;
  double          samprate;
  int64_t         samplecnt;
  void           *datasamples;
  uint64_t        datasize;
  int64_t         numsamples;
  char            sampletype;
  void           *prvtptr;
  struct MS3RecordList *recordlist;
  struct MS3TraceSeg *prev;
  struct MS3TraceSeg *next;
} MS3TraceSeg;

typedef struct MS3TraceID {
  char            sid[LM_SIDLEN];
  uint8_t         pubversion;
  nstime_t        earliest;
  nstime_t        latest;
  void           *prvtptr;
  uint32_t        numsegments;
  struct MS3TraceSeg *first;
  struct MS3TraceSeg *last;
  struct MS3TraceID *next[MSTRACEID_SKIPLIST_HEIGHT];
  uint8_t         height;
} MS3TraceID;

typedef struct MS3TraceList {
  uint32_t           numtraceids;
  struct MS3TraceID  traces;
  uint64_t           prngstate;
} MS3TraceList;

typedef struct MS3Tolerance
{
  double (*time) (const MS3Record *msr);
  double (*samprate) (const MS3Record *msr);
} MS3Tolerance;

typedef struct LMIO
{
  enum {
    LMIO_NULL = 0,
    LMIO_FILE = 1,
    LMIO_URL  = 2,
    LMIO_FD   = 3
  } type;
  void *handle;
  void *handle2;
  int still_running;
} LMIO;

typedef struct MS3FileParam
{
  char path[512];
  int64_t startoffset;
  int64_t endoffset;
  int64_t streampos;
  int64_t recordcount;

  char *readbuffer;
  int readlength;
  int readoffset;
  uint32_t flags;
  LMIO input;
} MS3FileParam;


// Functions
extern int ms_nstime2time (nstime_t nstime, uint16_t *year, uint16_t *yday,
                           uint8_t *hour, uint8_t *min, uint8_t *sec, uint32_t *nsec);
extern char* ms_nstime2timestr_n (nstime_t nstime, char *timestr, size_t timestrsize,
                                  ms_timeformat_t timeformat, ms_subseconds_t subsecond);
extern nstime_t ms_time2nstime (int year, int yday, int hour, int min, int sec, uint32_t nsec);
extern nstime_t ms_timestr2nstime (const char *timestr);
extern nstime_t ms_mdtimestr2nstime (const char *timestr);
extern nstime_t ms_seedtimestr2nstime (const char *seedtimestr);
extern int ms_doy2md (int year, int yday, int *month, int *mday);
extern int ms_md2doy (int year, int month, int mday, int *yday);


extern int msr3_parse (const char *record, uint64_t recbuflen, MS3Record **ppmsr,
                       uint32_t flags, int8_t verbose);

extern int msr3_pack (const MS3Record *msr,
                      void (*record_handler) (char *, int, void *),
                      void *handlerdata, int64_t *packedsamples,
                      uint32_t flags, int8_t verbose);

extern int msr3_repack_mseed3 (const MS3Record *msr, char *record, uint32_t recbuflen, int8_t verbose);

extern int msr3_repack_mseed2 (const MS3Record *msr, char *record, uint32_t recbuflen, int8_t verbose);

extern int msr3_pack_header3 (const MS3Record *msr, char *record, uint32_t recbuflen, int8_t verbose);

extern int msr3_pack_header2 (const MS3Record *msr, char *record, uint32_t recbuflen, int8_t verbose);

extern int64_t msr3_unpack_data (MS3Record *msr, int8_t verbose);

extern int msr3_data_bounds (const MS3Record *msr, uint32_t *dataoffset, uint32_t *datasize);

extern int64_t ms_decode_data (const void *input, uint64_t inputsize, uint8_t encoding,
                               uint64_t samplecount, void *output, uint64_t outputsize,
                               char *sampletype, int8_t swapflag, const char *sid, int8_t verbose);

extern MS3Record* msr3_init (MS3Record *msr);
extern void       msr3_free (MS3Record **ppmsr);
extern MS3Record* msr3_duplicate (const MS3Record *msr, int8_t datadup);
extern nstime_t   msr3_endtime (const MS3Record *msr);
extern void       msr3_print (const MS3Record *msr, int8_t details);
extern int        msr3_resize_buffer (MS3Record *msr);
extern double     msr3_sampratehz (const MS3Record *msr);
extern nstime_t   msr3_nsperiod (const MS3Record *msr);
extern double     msr3_host_latency (const MS3Record *msr);

extern int64_t ms3_detect (const char *record, uint64_t recbuflen, uint8_t *formatversion);
extern int ms_parse_raw3 (const char *record, int maxreclen, int8_t details);
extern int ms_parse_raw2 (const char *record, int maxreclen, int8_t details, int8_t swapflag);

extern const MS3Selections* ms3_matchselect (const MS3Selections *selections, const char *sid,
                                             nstime_t starttime, nstime_t endtime,
                                             int pubversion, const MS3SelectTime **ppselecttime);
extern const MS3Selections* msr3_matchselect (const MS3Selections *selections, const MS3Record *msr,
                                              const MS3SelectTime **ppselecttime);
extern int ms3_addselect (MS3Selections **ppselections, const char *sidpattern,
                          nstime_t starttime, nstime_t endtime, uint8_t pubversion);
extern int ms3_addselect_comp (MS3Selections **ppselections,
                               char *network, char* station, char *location, char *channel,
                               nstime_t starttime, nstime_t endtime, uint8_t pubversion);
extern int ms3_readselectionsfile (MS3Selections **ppselections, const char *filename);
extern void ms3_freeselections (MS3Selections *selections);
extern void ms3_printselections (const MS3Selections *selections);

extern MS3TraceList* mstl3_init (MS3TraceList *mstl);
extern void          mstl3_free (MS3TraceList **ppmstl, int8_t freeprvtptr);
extern MS3TraceID*   mstl3_findID (MS3TraceList *mstl, const char *sid, uint8_t pubversion, MS3TraceID **prev);

extern MS3TraceSeg*  mstl3_addmsr_recordptr (MS3TraceList *mstl, const MS3Record *msr, MS3RecordPtr **pprecptr,
                                             int8_t splitversion, int8_t autoheal, uint32_t flags,
                                             const MS3Tolerance *tolerance);
extern int64_t       mstl3_readbuffer (MS3TraceList **ppmstl, const char *buffer, uint64_t bufferlength,
                                       int8_t splitversion, uint32_t flags,
                                       const MS3Tolerance *tolerance, int8_t verbose);
extern int64_t       mstl3_readbuffer_selection (MS3TraceList **ppmstl, const char *buffer, uint64_t bufferlength,
                                                 int8_t splitversion, uint32_t flags,
                                                 const MS3Tolerance *tolerance, const MS3Selections *selections,
                                                 int8_t verbose);
extern int64_t mstl3_unpack_recordlist (MS3TraceID *id, MS3TraceSeg *seg, void *output,
                                        uint64_t outputsize, int8_t verbose);
extern int mstl3_convertsamples (MS3TraceSeg *seg, char type, int8_t truncate);
extern int mstl3_resize_buffers (MS3TraceList *mstl);
extern int64_t mstl3_pack (MS3TraceList *mstl, void (*record_handler) (char *, int, void *),
                           void *handlerdata, int reclen, int8_t encoding,
                           int64_t *packedsamples, uint32_t flags, int8_t verbose, char *extra);
extern int64_t mstl3_pack_ppupdate_flushidle (MS3TraceList *mstl,
                                              void (*record_handler) (char *, int, void *),
                                              void *handlerdata, int reclen, int8_t encoding,
                                              int64_t *packedsamples, uint32_t flags,
                                              int8_t verbose, char *extra,
                                              uint32_t flush_idle_seconds);
extern int64_t mstl3_pack_segment (MS3TraceList *mstl, MS3TraceID *id, MS3TraceSeg *seg,
                                   void (*record_handler) (char *, int, void *),
                                   void *handlerdata, int reclen, int8_t encoding,
                                   int64_t *packedsamples, uint32_t flags, int8_t verbose,
                                   char *extra);
extern void mstl3_printtracelist (const MS3TraceList *mstl, ms_timeformat_t timeformat,
                                  int8_t details, int8_t gaps, int8_t versions);
extern void mstl3_printsynclist (const MS3TraceList *mstl, const char *dccid, ms_subseconds_t subseconds);
extern void mstl3_printgaplist (const MS3TraceList *mstl, ms_timeformat_t timeformat,
                                double *mingap, double *maxgap);

extern int ms3_readmsr (MS3Record **ppmsr, const char *mspath, uint32_t flags, int8_t verbose);
extern int ms3_readmsr_r (MS3FileParam **ppmsfp, MS3Record **ppmsr, const char *mspath,
                          uint32_t flags, int8_t verbose);
extern int ms3_readmsr_selection (MS3FileParam **ppmsfp, MS3Record **ppmsr, const char *mspath,
                                  uint32_t flags, const MS3Selections *selections, int8_t verbose);
extern int ms3_readtracelist (MS3TraceList **ppmstl, const char *mspath, const MS3Tolerance *tolerance,
                              int8_t splitversion, uint32_t flags, int8_t verbose);
extern int ms3_readtracelist_timewin (MS3TraceList **ppmstl, const char *mspath, const MS3Tolerance *tolerance,
                                      nstime_t starttime, nstime_t endtime, int8_t splitversion, uint32_t flags,
                                      int8_t verbose);
extern int ms3_readtracelist_selection (MS3TraceList **ppmstl, const char *mspath, const MS3Tolerance *tolerance,
                                        const MS3Selections *selections, int8_t splitversion, uint32_t flags, int8_t verbose);
extern int ms3_url_useragent (const char *program, const char *version);
extern int ms3_url_userpassword (const char *userpassword);
extern int ms3_url_addheader (const char *header);
extern void ms3_url_freeheaders (void);
extern int64_t msr3_writemseed (MS3Record *msr, const char *mspath, int8_t overwrite,
                                uint32_t flags, int8_t verbose);
extern int64_t mstl3_writemseed (MS3TraceList *mstl, const char *mspath, int8_t overwrite,
                                 int maxreclen, int8_t encoding, uint32_t flags, int8_t verbose);
extern int libmseed_url_support (void);
extern MS3FileParam *ms3_mstl_init_fd (int fd);

extern int ms_sid2nslc_n (const char *sid,
                          char *net, size_t netsize, char *sta, size_t stasize,
                          char *loc, size_t locsize, char *chan, size_t chansize);
extern int ms_nslc2sid (char *sid, int sidlen, uint16_t flags,
                        const char *net, const char *sta, const char *loc, const char *chan);
extern int ms_seedchan2xchan (char *xchan, const char *seedchan);
extern int ms_xchan2seedchan (char *seedchan, const char *xchan);
extern int ms_strncpclean (char *dest, const char *source, int length);
extern int ms_strncpcleantail (char *dest, const char *source, int length);
extern int ms_strncpopen (char *dest, const char *source, int length);

extern int mseh_replace (MS3Record *msr, char *jsonstring);

extern uint8_t ms_samplesize (char sampletype);
extern int ms_encoding_sizetype (uint8_t encoding, uint8_t *samplesize, char *sampletype);
extern const char *ms_encodingstr (uint8_t encoding);
extern const char *ms_errorstr (int errorcode);

extern nstime_t ms_sampletime (nstime_t time, int64_t offset, double samprate);
extern int ms_bigendianhost (void);

extern nstime_t lmp_systemtime (void);
"""

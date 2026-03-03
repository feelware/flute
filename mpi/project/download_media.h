#pragma once
#include <stddef.h>

#define MINIO_ENDPOINT "http://minio:9000"
#define MINIO_BUCKET "uploads"
#define NUM_DOWNLOAD_THREADS 4
#define CHUNK_SIZE (1024 * 1024)  // 1 MB chunks

typedef struct {
    char *data;
    size_t size;
    size_t capacity;
} MemoryBuffer;

typedef struct {
    char *url;
    long start_byte;
    long end_byte;
    char *output_buffer;
    size_t bytes_downloaded;
    int thread_id;
    int success;
} DownloadChunk;

size_t write_memory_callback(void *contents, size_t size, size_t nmemb, void *userp);
long get_file_size(const char *url);
char *generate_presigned_url(const char *bucket, const char *object_key);
int download_video_parallel(const char *video_path, char *output_file, int rank);
int download_lut(const char *lut_path, char *output_file);
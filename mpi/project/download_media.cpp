#include "download_media.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <curl/curl.h>
#include <unistd.h>
#include <sys/stat.h>

size_t write_memory_callback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    MemoryBuffer *mem = (MemoryBuffer *)userp;

    if (mem->size + realsize > mem->capacity) {
        size_t new_capacity = mem->capacity * 2;
        if (new_capacity < mem->size + realsize) {
            new_capacity = mem->size + realsize;
        }
        char *new_data = (char *)malloc(new_capacity);
        if (new_data == NULL) {
            fprintf(stderr, "Error: No se pudo realocar memoria\n");
            return 0;
        }
        mem->data = new_data;
        mem->capacity = new_capacity;
    }

    memcpy(&(mem->data[mem->size]), contents, realsize);
    mem->size += realsize;
    return realsize;
}

long get_file_size(const char *url) {
    CURL *curl;
    CURLcode res;
    long file_size = -1;

    curl = curl_easy_init();
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, NULL);

        res = curl_easy_perform(curl);

        if (res == CURLE_OK) {
            double content_length;
            res = curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &content_length);
            if (res == CURLE_OK && content_length > 0) {
                file_size = (long)content_length;
            }
        }

        curl_easy_cleanup(curl);
    }

    return file_size;
}

void *download_chunk_thread(void *arg) {
    DownloadChunk *chunk = (DownloadChunk *)arg;
    CURL *curl;
    CURLcode res;
    MemoryBuffer mem = {0};

    mem.capacity = chunk->end_byte - chunk->start_byte + 1;
    mem.data = (char *)malloc(mem.capacity);
    if (mem.data == NULL) {
        fprintf(stderr, "Thread %d: Error al asignar memoria\n", chunk->thread_id);
        chunk->success = 0;
        return NULL;
    }

    curl = curl_easy_init();
    if (!curl) {
        fprintf(stderr, "Thread %d: Error al inicializar curl\n", chunk->thread_id);
        free(mem.data);
        chunk->success = 0;
        return NULL;
    }

    char range[128];
    snprintf(range, sizeof(range), "%ld-%ld", chunk->start_byte, chunk->end_byte);

    curl_easy_setopt(curl, CURLOPT_URL, chunk->url);
    curl_easy_setopt(curl, CURLOPT_RANGE, range);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_memory_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&mem);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);

    printf("Thread %d: Descargando bytes %ld-%ld (%ld bytes)\n",
           chunk->thread_id, chunk->start_byte, chunk->end_byte,
           chunk->end_byte - chunk->start_byte + 1);
    fflush(stdout);

    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
        fprintf(stderr, "Thread %d: Error en descarga: %s\n",
                chunk->thread_id, curl_easy_strerror(res));
        chunk->success = 0;
    } else {
        chunk->output_buffer = mem.data;
        chunk->bytes_downloaded = mem.size;
        chunk->success = 1;
        printf("Thread %d: Descarga completada (%zu bytes)\n",
               chunk->thread_id, mem.size);
        fflush(stdout);
    }

    curl_easy_cleanup(curl);
    return NULL;
}

char *generate_presigned_url(const char *bucket, const char *object_key) {
    char *presigned_url = (char *)malloc(1024);
    if (!presigned_url) {
        fprintf(stderr, "Error al asignar memoria para URL\n");
        return NULL;
    }

    snprintf(presigned_url, 1024, "%s/%s/%s", MINIO_ENDPOINT, bucket, object_key);

    printf("URL publica generada: %s\n", presigned_url);
    fflush(stdout);

    return presigned_url;
}

int download_lut(const char *lut_path, char *output_file) {
    char bucket[256];
    char object_key[512];

    if (sscanf(lut_path, "%255[^/]/%511s", bucket, object_key) != 2) {
        fprintf(stderr, "Error: lut_path no tiene formato bucket/object: %s\n", lut_path);
        return 0;
    }

    char *url = generate_presigned_url(bucket, object_key);
    if (!url) {
        return 0;
    }

    CURL *curl = curl_easy_init();
    if (!curl) {
        fprintf(stderr, "Error al inicializar curl\n");
        free(url);
        return 0;
    }

    FILE *fp = fopen(output_file, "wb");
    if (!fp) {
        fprintf(stderr, "Error: No se pudo crear el archivo de salida: %s\n", output_file);
        curl_easy_cleanup(curl);
        free(url);
        return 0;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, NULL);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);

    printf("Descargando LUT desde MinIO...\n");
    fflush(stdout);

    CURLcode res = curl_easy_perform(curl);

    fclose(fp);
    curl_easy_cleanup(curl);
    free(url);

    if (res != CURLE_OK) {
        fprintf(stderr, "Error en descarga de LUT: %s\n", curl_easy_strerror(res));
        return 0;
    }

    printf("Descarga de LUT completada exitosamente: %s\n", output_file);
    fflush(stdout);

    return 1;
}

int download_video_parallel(const char *video_path, char *output_file, int rank) {
    if (rank != 0) {
        return 1;
    }

    char bucket[256];
    char object_key[512];

    if (sscanf(video_path, "%255[^/]/%511s", bucket, object_key) != 2) {
        fprintf(stderr, "Error: video_path no tiene formato bucket/object: %s\n", video_path);
        return 0;
    }

    char *url = generate_presigned_url(bucket, object_key);
    if (!url) {
        return 0;
    }

    printf("Obteniendo tamano del archivo...\n");
    fflush(stdout);

    long file_size = get_file_size(url);
    if (file_size <= 0) {
        fprintf(stderr, "Error: No se pudo obtener el tamano del archivo\n");
        free(url);
        return 0;
    }

    printf("Tamano del archivo: %ld bytes (%.2f MB)\n",
           file_size, file_size / (1024.0 * 1024.0));
    fflush(stdout);

    int num_threads = NUM_DOWNLOAD_THREADS;
    long chunk_size = file_size / num_threads;

    DownloadChunk *chunks = (DownloadChunk *)malloc(num_threads * sizeof(DownloadChunk));
    pthread_t *threads = (pthread_t *)malloc(num_threads * sizeof(pthread_t));

    if (!chunks || !threads) {
        fprintf(stderr, "Error al asignar memoria para threads\n");
        free(url);
        free(chunks);
        free(threads);
        return 0;
    }

    printf("Iniciando descarga paralela con %d threads...\n", num_threads);
    fflush(stdout);

    for (int i = 0; i < num_threads; i++) {
        chunks[i].url = url;
        chunks[i].thread_id = i;
        chunks[i].start_byte = i * chunk_size;
        chunks[i].end_byte = (i == num_threads - 1) ? file_size - 1 : (i + 1) * chunk_size - 1;
        chunks[i].output_buffer = NULL;
        chunks[i].bytes_downloaded = 0;
        chunks[i].success = 0;

        if (pthread_create(&threads[i], NULL, download_chunk_thread, &chunks[i]) != 0) {
            fprintf(stderr, "Error al crear thread %d\n", i);
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j], NULL);
            }
            free(url);
            free(chunks);
            free(threads);
            return 0;
        }
    }

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    printf("Todos los threads completados, ensamblando archivo...\n");
    fflush(stdout);

    int all_success = 1;
    for (int i = 0; i < num_threads; i++) {
        if (!chunks[i].success) {
            fprintf(stderr, "Error: Thread %d fallo en la descarga\n", i);
            all_success = 0;
        }
    }

    if (!all_success) {
        for (int i = 0; i < num_threads; i++) {
            if (chunks[i].output_buffer) {
                free(chunks[i].output_buffer);
            }
        }
        free(url);
        free(chunks);
        free(threads);
        return 0;
    }

    FILE *fp = fopen(output_file, "wb");
    if (!fp) {
        fprintf(stderr, "Error: No se pudo crear el archivo de salida: %s\n", output_file);
        for (int i = 0; i < num_threads; i++) {
            if (chunks[i].output_buffer) {
                free(chunks[i].output_buffer);
            }
        }
        free(url);
        free(chunks);
        free(threads);
        return 0;
    }

    for (int i = 0; i < num_threads; i++) {
        if (fwrite(chunks[i].output_buffer, 1, chunks[i].bytes_downloaded, fp) != chunks[i].bytes_downloaded) {
            fprintf(stderr, "Error al escribir chunk %d al archivo\n", i);
            fclose(fp);
            for (int j = 0; j < num_threads; j++) {
                if (chunks[j].output_buffer) {
                    free(chunks[j].output_buffer);
                }
            }
            free(url);
            free(chunks);
            free(threads);
            return 0;
        }
    }

    fclose(fp);

    for (int i = 0; i < num_threads; i++) {
        if (chunks[i].output_buffer) {
            free(chunks[i].output_buffer);
        }
    }

    free(url);
    free(chunks);
    free(threads);

    printf("Descarga completada exitosamente: %s\n", output_file);
    fflush(stdout);

    return 1;
}
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <curl/curl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <omp.h>
#include "apply_lut.h"
#include "download_media.h"
#include <vector>
#include <opencv2/opencv.hpp>
#include <fstream>

using namespace cv;

std::vector<Mat> read_frames(const char* video_path, int start, int end) {
    std::vector<Mat> frames;
    VideoCapture cap(video_path);
    if (!cap.isOpened()) {
        fprintf(stderr, "Error: No se pudo abrir el video %s\n", video_path);
        return frames;
    }

    cap.set(CAP_PROP_POS_FRAMES, start);
    for (int i = start; i < end; i++) {
        Mat frame;
        cap >> frame;
        if (frame.empty()) break;
        frames.push_back(frame);
    }
    return frames;
}

void write_frames(const char* output_path, const std::vector<Mat>& frames, double fps, Size frame_size) {
    int fourcc = VideoWriter::fourcc('M', 'J', 'P', 'G');
    VideoWriter writer(output_path, fourcc, fps, frame_size);
    if (!writer.isOpened()) {
        fprintf(stderr, "Error: No se pudo abrir el archivo de salida %s\n", output_path);
        return;
    }

    for (const auto& frame : frames) {
        writer.write(frame);
    }
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int rank, num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    if (argc < 4) {
        if (rank == 0) {
            fprintf(stderr, "Usage: %s <job_id> <in_video_storage_path> <lut_storage_path> [enable_intra_node_parallelism]\n", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    const char *job_id = argv[1];
    const char *in_video_storage_path = argv[2];
    const char *lut_storage_path = argv[3];
    
    // Parse intra-node parallelism flag (default to true for backward compatibility)
    int enable_intra_node_parallelism = 1;
    if (argc >= 5) {
        enable_intra_node_parallelism = (atoi(argv[4]) != 0) ? 1 : 0;
    }
    
    // Disable OpenMP if intra-node parallelism is disabled
    if (!enable_intra_node_parallelism) {
        omp_set_num_threads(1);
        if (rank == 0) {
            printf("Intra-node parallelism disabled (OpenMP threads set to 1)\n");
        }
    } else {
        if (rank == 0) {
            printf("Intra-node parallelism enabled\n");
        }
    }
    fflush(stdout);

    char in_video_path[512];
    snprintf(in_video_path, sizeof(in_video_path), "/tmp/video_%s.mp4", job_id);

    std::vector<Mat> frames;
    int total_frames = 0;
    double fps = 0.0;
    Size frame_size;

    if (rank == 0) {
        printf("========================================\n");
        printf("PROCESS_VIDEO - Iniciando procesamiento\n");
        printf("========================================\n");
        printf("Job ID: %s\n", job_id);
        printf("Video Path: %s\n", in_video_storage_path);
        printf("LUT Path: %s\n", lut_storage_path);
        printf("MPI Processes: %d\n", num_procs);
        printf("========================================\n\n");
        fflush(stdout);

        curl_global_init(CURL_GLOBAL_DEFAULT);

        printf("Descargando video desde MinIO...\n");
        fflush(stdout);

        if (!download_video_parallel(in_video_storage_path, in_video_path, rank)) {
            fprintf(stderr, "Error: Fallo la descarga del video\n");
            curl_global_cleanup();
            MPI_Finalize();
            return 1;
        }

        printf("\n========================================\n");
        printf("Descarga completada - Video disponible en: %s\n", in_video_path);
        printf("========================================\n\n");
        fflush(stdout);

        // Open the video file to get its properties
        VideoCapture cap(in_video_path);
        if (!cap.isOpened()) {
            fprintf(stderr, "Error: No se pudo abrir el video %s\n", in_video_path);
            curl_global_cleanup();
            MPI_Finalize();
            return 1;
        }

        total_frames = static_cast<int>(cap.get(CAP_PROP_FRAME_COUNT));
        fps = cap.get(CAP_PROP_FPS);
        frame_size = Size(
            static_cast<int>(cap.get(CAP_PROP_FRAME_WIDTH)),
            static_cast<int>(cap.get(CAP_PROP_FRAME_HEIGHT))
        );

        printf("Total frames: %d\n", total_frames);

        // Calculate the number of frames per process
        int frames_per_proc = total_frames / num_procs;
        int remainder = total_frames % num_procs;

        // Split the video into portions
        std::vector<std::vector<int>> frame_ranges(num_procs);
        int start_frame = 0;
        for (int i = 0; i < num_procs; i++) {
            int end_frame = start_frame + frames_per_proc + (i < remainder ? 1 : 0);
            frame_ranges[i] = {start_frame, end_frame};
            start_frame = end_frame;
        }

        // Serialize the video portions for sending with optional OpenMP parallelization
        std::vector<std::vector<std::vector<uchar>>> serialized_portions(num_procs);
        if (enable_intra_node_parallelism) {
            #pragma omp parallel for shared(serialized_portions)
            for (int i = 1; i < num_procs; i++) {
                std::vector<Mat> process_frames = read_frames(in_video_path, frame_ranges[i][0], frame_ranges[i][1]);
                for (const auto& frame : process_frames) {
                    std::vector<uchar> buffer;
                    imencode(".jpg", frame, buffer);
                    #pragma omp critical
                    serialized_portions[i].push_back(buffer);
                }
            }
        } else {
            // Sequential version when parallelism is disabled
            for (int i = 1; i < num_procs; i++) {
                std::vector<Mat> process_frames = read_frames(in_video_path, frame_ranges[i][0], frame_ranges[i][1]);
                for (const auto& frame : process_frames) {
                    std::vector<uchar> buffer;
                    imencode(".jpg", frame, buffer);
                    serialized_portions[i].push_back(buffer);
                }
            }
        }

        // Send serialized portions to other processes
        for (int i = 1; i < num_procs; i++) {
            int num_frames = serialized_portions[i].size();
            MPI_Send(&num_frames, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            for (const auto& frame_buffer : serialized_portions[i]) {
                int frame_size = frame_buffer.size();
                MPI_Send(&frame_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                MPI_Send(frame_buffer.data(), frame_size, MPI_CHAR, i, 0, MPI_COMM_WORLD);
            }
        }

        // Process 0 keeps its own frames
        frames = read_frames(in_video_path, frame_ranges[0][0], frame_ranges[0][1]);
    } else {
        // Receive serialized portion from process 0
        int num_frames;
        MPI_Recv(&num_frames, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        for (int i = 0; i < num_frames; i++) {
            int frame_size;
            MPI_Recv(&frame_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            std::vector<uchar> frame_buffer(frame_size);
            MPI_Recv(frame_buffer.data(), frame_size, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            Mat frame = imdecode(frame_buffer, IMREAD_COLOR);
            if (frame.empty()) {
                fprintf(stderr, "Error: No se pudo decodificar un frame en process %d\n", rank);
                continue;
            }
            frames.push_back(frame);
        }
    }

    printf("Process %d received %lu frames\n", rank, frames.size());

    char lut_path[512];
    snprintf(lut_path, sizeof(lut_path), "/tmp/lut_%s.png", job_id);

    printf("Descargando LUT desde MinIO...\n");
    fflush(stdout);

    if (!download_lut(lut_storage_path, lut_path)) {
        fprintf(stderr, "Error: Fallo la descarga de la LUT\n");
        MPI_Finalize();
        return 1;
    }

    std::vector<Mat> processed_frames;
    if (!apply_lut_from_buffer(frames, processed_frames, lut_path)) {
        fprintf(stderr, "Error: Fallo el procesamiento de frames en process %d\n", rank);
        MPI_Finalize();
        return 1;
    }

    printf("Process %d procesó %lu frames\n", rank, processed_frames.size());

    // Serialize processed frames for sending back to process 0

    if (rank != 0) {
        std::vector<std::vector<uchar>> serialized_frames;
        for (const auto& frame : processed_frames) {
            std::vector<uchar> buffer;
            imencode(".jpg", frame, buffer);
            serialized_frames.push_back(buffer);
        }

        // Send processed frames back to process 0
        int num_frames = serialized_frames.size();
        MPI_Send(&num_frames, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        for (const auto& frame_buffer : serialized_frames) {
            int frame_size = frame_buffer.size();
            MPI_Send(&frame_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            MPI_Send(frame_buffer.data(), frame_size, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        }
    } else {
        std::vector<Mat> all_processed_frames = processed_frames; // Start with its own processed frames
        for (int i = 1; i < num_procs; i++) {
            int num_frames;
            MPI_Recv(&num_frames, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (int j = 0; j < num_frames; j++) {
                int frame_size;
                MPI_Recv(&frame_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                std::vector<uchar> frame_buffer(frame_size);
                MPI_Recv(frame_buffer.data(), frame_size, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                Mat frame = imdecode(frame_buffer, IMREAD_COLOR);
                if (frame.empty()) {
                    fprintf(stderr, "Error: No se pudo decodificar un frame procesado en process %d\n", rank);
                    continue;
                }
                all_processed_frames.push_back(frame);
            }
        }

        printf("Process %d received a total of %lu processed frames\n", rank, all_processed_frames.size());

        char output_path[512];
        snprintf(output_path, sizeof(output_path), "processed_video_%s.avi", job_id);
        write_frames(output_path, all_processed_frames, fps, frame_size);

        printf("Process %d escribió el video procesado a: %s\n", rank, output_path);
    }

    MPI_Finalize();
    return 0;
}

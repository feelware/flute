#include <mpi.h>
#include <omp.h>
#include <opencv2/opencv.hpp>
#include <opencv2/videoio.hpp>
#include <stdio.h>
#include <stdlib.h>
#include <vector>

using namespace cv;

Vec3b trilinear_interp(const Mat &lut, float r, float g, float b, int n) {
    int r0 = (int)r, r1 = std::min(r0+1, n-1);
    int g0 = (int)g, g1 = std::min(g0+1, n-1);
    int b0 = (int)b, b1 = std::min(b0+1, n-1);
    float fr = r-r0, fg = g-g0, fb = b-b0;
    Vec3b c000=lut.at<Vec3b>(r0,g0,b0), c001=lut.at<Vec3b>(r0,g0,b1);
    Vec3b c010=lut.at<Vec3b>(r0,g1,b0), c011=lut.at<Vec3b>(r0,g1,b1);
    Vec3b c100=lut.at<Vec3b>(r1,g0,b0), c101=lut.at<Vec3b>(r1,g0,b1);
    Vec3b c110=lut.at<Vec3b>(r1,g1,b0), c111=lut.at<Vec3b>(r1,g1,b1);
    Vec3b res;
    for (int ch = 0; ch < 3; ch++)
        res[ch] = (uchar)std::round(
            c000[ch]*(1-fr)*(1-fg)*(1-fb) + c001[ch]*(1-fr)*(1-fg)*fb +
            c010[ch]*(1-fr)*fg*(1-fb)     + c011[ch]*(1-fr)*fg*fb     +
            c100[ch]*fr*(1-fg)*(1-fb)     + c101[ch]*fr*(1-fg)*fb     +
            c110[ch]*fr*fg*(1-fb)         + c111[ch]*fr*fg*fb);
        return res;
}

int apply_lut_from_buffer(const std::vector<Mat>& frames, std::vector<Mat>& processed_frames, const char* lut_path) {
    Mat raw_lut = imread(lut_path, IMREAD_COLOR);

    if (raw_lut.empty() || raw_lut.rows != raw_lut.cols) {
        fprintf(stderr, "Error cargando LUT\n");
        return 0;
    }
    int img_size = raw_lut.rows;
    int bpr = (int)std::round(std::cbrt((double)img_size));
    int n   = img_size / bpr;
    int sizes[3] = {n, n, n};
    Mat lut(3, sizes, CV_8UC3);
    
    #pragma omp parallel for collapse(3)
    for (int ri = 0; ri < n; ri++)
        for (int gi = 0; gi < n; gi++)
            for (int bi = 0; bi < n; bi++)
                lut.at<Vec3b>(ri, gi, bi) = raw_lut.at<Vec3b>((bi/bpr)*n+gi, (bi%bpr)*n+ri);

    processed_frames.resize(frames.size());
    float scale = (n-1) / 255.0f;
    
    #pragma omp parallel for schedule(dynamic)
    for (size_t frame_idx = 0; frame_idx < frames.size(); frame_idx++) {
        const Mat& in_frame = frames[frame_idx];
        
        if (in_frame.empty()) {
            fprintf(stderr, "Error: Frame vacío encontrado\n");
            continue;
        }

        Mat output_frame = Mat(in_frame.size(), in_frame.type());
        
        #pragma omp parallel for collapse(2) schedule(static)
        for (int y = 0; y < in_frame.rows; y++) {
            for (int x = 0; x < in_frame.cols; x++) {
                Vec3b p = in_frame.at<Vec3b>(y, x);
                output_frame.at<Vec3b>(y, x) = trilinear_interp(lut, p[2]*scale, p[1]*scale, p[0]*scale, n);
            }
        }

        processed_frames[frame_idx] = output_frame;
    }

    return 1;
}
#pragma once
#include <opencv2/core.hpp>
#include <vector>

using namespace cv;

int apply_lut_from_buffer(const std::vector<Mat>& frames, std::vector<Mat>& processed_frames, const char* lut_path);

#pragma once
#ifndef __COMMON_ZIPF_DISTRIBUTION_H__
#define __COMMON_ZIPF_DISTRIBUTION_H__

#include <cstdint>
#include <cmath>
#include <random>
#include "FastRandom.h"

class ZipfDistribution{
public:
	ZipfDistribution(const uint64_t& n, const double& theta) : rand_generator(rand()){
		// range: 1-n
		the_n = n;
		zipf_theta = theta;
		zeta_2_theta = zeta(2, zipf_theta);
		denom = zeta(the_n, zipf_theta);
	}
	double zeta(uint64_t n, double theta) {
		double sum = 0;
		for (uint64_t i = 1; i <= n; i++)
			sum += pow(1.0 / i, theta);
		return sum;
	}
	int GenerateInteger(const int &min, const int &max){
		return rand_generator.next() % (max - min + 1) + min;
		//// TODO: needs a better, cross-platform random generator!
		//if (RAND_MAX <= 32767){
		//	return ((rand() << 16) + rand()) % (max - min + 1) + min;
		//}
		//else{
		//	return rand() % (max - min + 1) + min;
		//}
	}
	uint64_t GetNextNumber() {
		double alpha = 1 / (1 - zipf_theta);
		double zetan = denom;
		double eta = (1 - pow(2.0 / the_n, 1 - zipf_theta)) /
			(1 - zeta_2_theta / zetan);
		double u = (double)(GenerateInteger(1, 10000000) % 10000000) / 10000000;
		double uz = u * zetan;
		if (uz < 1) return 1;
		if (uz < 1 + pow(0.5, zipf_theta)) return 2;
		return 1 + (uint64_t)(the_n * pow(eta*u - eta + 1, alpha));
	}

	uint64_t the_n;
	double zipf_theta;
	double denom;
	double zeta_2_theta;
	fast_random rand_generator;
};

#endif
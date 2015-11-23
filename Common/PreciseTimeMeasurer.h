#pragma once
#ifndef __COMMON_PRECISE_TIME_MEASURER_H__
#define __COMMON_PRECISE_TIME_MEASURER_H__

// for tembusu.
static const float CPU_MHZ = 1400;
class PreciseTimeMeasurer{
public:
	PreciseTimeMeasurer(){}
	~PreciseTimeMeasurer(){}

//#if defined(__x86_64__)
	static __inline__ unsigned long long rdtsc(void){
		unsigned hi, lo;
		__asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
		return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
	}
//#endif

	void StartTimer(){
		start_time_ = rdtsc();
	}

	void EndTimer(){
		end_time_ = rdtsc();
	}
	
	static float get_run_time(const int &start, const int &end, const float &cpu_mhz){
		return (end - start) / (cpu_mhz * 1000 * 1000);
	}

	long long GetElapsedMilliSeconds(){
		return (long long)((end_time_ - start_time_) / (CPU_MHZ * 1000));
	}

	long long GetElapsedMicroSeconds(){
		return (long long)((end_time_ - start_time_) / CPU_MHZ);
	}

	long long GetElapsedNanoSeconds(){
		return (long long)((end_time_ - start_time_) * 1000 / CPU_MHZ);
	}

private:
	PreciseTimeMeasurer(const PreciseTimeMeasurer&);
	PreciseTimeMeasurer& operator=(const PreciseTimeMeasurer&);

private:
	unsigned long long start_time_;
	unsigned long long end_time_;
};

#endif

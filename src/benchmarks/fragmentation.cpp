/*
 * Copyright 2018, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * fragmentation.cpp -- fragmentation benchmarks definitions
 */

#include <cassert>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>//to delete

#include "benchmark.hpp"
//#include "fragmentation_worker.hpp"
#include "file.h"
#include "libpmemobj.h"
#include "os.h"
#include "poolset_util.hpp"

 /*
 * The factor used for PMEM pool size calculation, accounts for metadata,
 * fragmentation and etc.
 */
#define FACTOR 1.2f

 /* The minimum allocation size that pmalloc can perform */
#define ALLOC_MIN_SIZE 64

 /* OOB and allocation header size */
#define OOB_HEADER_SIZE 64

/*
 * mem_usage -- specifies how memory will grow
 */
enum mem_usage_type {
	MEM_USAGE_UNKNOWN,
	MEM_USAGE_FLAT,
	MEM_USAGE_PEAK,
	MEM_USAGE_RAMP
};

static float fragmentation = 0;
static float fragmentation2 = 0;

/*
 * prog_args -- benchmark specific arguments
 */
struct prog_args {
	char *background_mem_usage_type_str; /* mem_usage_type: flat, peak, ramp */
	char *scenario;
	size_t start_obj_size;
	size_t max_obj_size;
	size_t growth_factor;
	size_t peak_multiplier;
	unsigned lifetime;
	unsigned peak_lifetime;
	unsigned peak_allocs;
};

struct frag_obj
{
	unsigned block_lifetime;
	size_t block_size;
	PMEMoid oid;
	bool is_allocated;

	void init(struct prog_args *args)
	{
		block_lifetime = args->lifetime;
		block_size = args->start_obj_size;
		is_allocated = false;
	}
};

struct frag_bench
{
	PMEMobjpool *pop; /* persistent pool handle */
	struct prog_args *pa; /* prog_args structure */
	mem_usage_type background_mem_usage;
	uint64_t n_ops;
	size_t poolsize;
	size_t theoretical_memory_usage;
	struct frag_obj **pmemobj_array;
	int(*func_op)(frag_obj * op_obj, frag_bench * fb, struct frag_worker * fworker, operation_info *info);

	void free_pmemobj_array(unsigned nb)
	{
		for (unsigned i = 0; i < nb && nb >= 0; i++)
		{
			if (pmemobj_array[i]->is_allocated)
			{
				pmemobj_free(&pmemobj_array[i]->oid);
			}
			free(pmemobj_array[i]);
		}
	}
};

/*
* struct frag_worker -- fragmentation worker context
*/
struct frag_worker {
	unsigned op_obj_off;
	size_t max_block_size;
	size_t cur_block_size;
	size_t growth_factor;
};

struct scenario {
	const char *scenario_name;
	int(*func_op)(frag_obj * op_obj, frag_bench * fb, struct frag_worker * fworker, operation_info *info);
};

/*
 * parse_memory_usage_type -- parse command line "--operation" argument
 *
 * Returns proper memory usage type.
 */
static mem_usage_type
parse_memory_usage_type(const char *arg)
{
	if (strcmp(arg, "flat") == 0)
		return MEM_USAGE_FLAT;
	else if (strcmp(arg, "peak") == 0)
		return MEM_USAGE_PEAK;
	else if (strcmp(arg, "ramp") == 0)
		return MEM_USAGE_RAMP;
	else
		return MEM_USAGE_UNKNOWN;
}

/*
* dealloc_after_obj_lifetime -- function deallocate memory after object lifetime except last operation
memory is not deallocated for purpose of fragmentation calculation at the end of benchmark execution
*/
static void
dealloc_after_obj_lifetime(frag_obj * op_obj, int op_index, int n_ops, size_t *mem_usage)
{
	sleep(op_obj->block_lifetime);
	if (op_index < n_ops - 1)
	{
		pmemobj_free(&op_obj->oid);
		*mem_usage -= op_obj->block_size;
		op_obj->is_allocated = false;
	}
}

static int
alloc_peak(frag_bench *fb)
{
	std::cout << "peak alloc\n";//todelete
	PMEMoid *oids;
	oids = (PMEMoid *)malloc(fb->pa->peak_allocs * sizeof(*oids));

	for (unsigned i = 0; i <fb->pa->peak_allocs; ++i)
	{
		if (pmemobj_alloc(fb->pop, &oids[i], ALLOC_MIN_SIZE, 0, nullptr,
			nullptr)) {
			perror("pmemobj_alloc");
			free(oids);
			return -1;
		}
	}

	usleep(fb->pa->peak_lifetime);

	for (unsigned i = 0; i < fb->pa->peak_allocs; ++i)
	{
		pmemobj_free(&oids[i]);
	}

	free(oids);
	return 0;
}

static int
background_allocations(frag_obj * op_obj, frag_bench * fb, struct frag_worker * worker, operation_info *info)
{
	if (pmemobj_alloc(fb->pop, &op_obj->oid, op_obj->block_size, 0, nullptr,
		nullptr)) {
		perror("pmemobj_alloc");
		return -1;
	}
	op_obj->is_allocated = true;
	fb->theoretical_memory_usage += op_obj->block_size;

	switch (fb->background_mem_usage)
	{
	case MEM_USAGE_FLAT:
		dealloc_after_obj_lifetime(op_obj, info->index, fb->n_ops, &fb->theoretical_memory_usage);
		break;
	case MEM_USAGE_RAMP:
		dealloc_after_obj_lifetime(op_obj, info->index, fb->n_ops, &fb->theoretical_memory_usage);
		if (worker->cur_block_size < worker->max_block_size)
		{
			worker->cur_block_size += worker->growth_factor;
			if (worker->cur_block_size > worker->max_block_size)
			{
				worker->cur_block_size = worker->max_block_size;
			}
		}
		break;
	case MEM_USAGE_PEAK:
		if (alloc_peak(fb))
		{
			return -1;
		}
		dealloc_after_obj_lifetime(op_obj, info->index, fb->n_ops, &fb->theoretical_memory_usage);
		break;
	default:
		break;
	}

	return 0;
}

static int
basic(frag_obj * op_obj, frag_bench * fb, struct frag_worker * fworker, operation_info *info)
{
	return background_allocations(op_obj, fb, fworker, info);
}

static int
add_peaks(frag_obj * op_obj, frag_bench * fb, struct frag_worker * fworker, operation_info *info)
{
	//struct fragmentatnion_worker *fworker = fragmentatnion_worker_alloc();
	if (background_allocations(op_obj, fb, fworker, info))
		return -1;

	return 0;
}
static struct scenario scenarios[] = {
	{ "basic", basic },
	{ "basic_with_peaks", add_peaks }
};

#define SCENARIOS (sizeof(scenarios) / sizeof(scenarios[0]))

static int
parse_scenario(const char *arg)
{
	for (unsigned i = 0; i < SCENARIOS; i++) {
		if (strcmp(arg, scenarios[i].scenario_name) == 0)
			return i;
	}
	return -1;
}

/*
 * frag_operation -- main operations for fragmentation benchmark
 */
static int
frag_operation(struct benchmark *bench, struct operation_info *info)
{
	auto *fb = (struct frag_bench *)pmembench_get_priv(bench);
	auto *fworker = (struct frag_worker *)info->worker->priv;
	auto *op_pmemobj = fb->pmemobj_array[fworker->op_obj_off + info->index];

	op_pmemobj->block_size = fworker->cur_block_size;
	std::cout << "op " << info->index << " thread " << info->worker->index << " block size " << fworker->cur_block_size << std::endl;//todelete

	return fb->func_op(op_pmemobj, fb, fworker, info);
}

/*
 * frag_init_worker -- init benchmark worker
 */
static int
frag_init_worker(struct benchmark *bench,
	struct benchmark_args *args,
	struct worker_info *worker)
{
	std::cout << "init worker " << worker->index << "\n";//todelete
	struct frag_worker *fworker =
		(struct frag_worker *)malloc(sizeof(*fworker));

	if (!fworker) {
		perror("malloc");
		return -1;
	}

	auto *fb = (struct frag_bench *)pmembench_get_priv(bench);

	fworker->op_obj_off = worker->index * args->n_ops_per_thread;
	fworker->cur_block_size = fb->pa->start_obj_size;
	fworker->max_block_size = fb->pa->max_obj_size;
	fworker->growth_factor = fb->pa->growth_factor;
	worker->priv = fworker;
	return 0;
}

/*
 * frag_free_worker -- cleanup benchmark worker
 */
static void
frag_free_worker(struct benchmark *bench,
	struct benchmark_args *args,
	struct worker_info *worker)
{
	auto *fworker = (struct frag_worker *)worker->priv;
	std::cout << "free worker " << worker->index << "\n";//todelete
	free(fworker);
}

/*
 * frag_init -- benchmark initialization function
 */
static int
frag_init(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench != nullptr);
	assert(args != nullptr);
	assert(args->opts != nullptr);
	std::cout << "frag init\n";//todelete
	int scenario_index;
	auto *fa = (struct prog_args *)args->opts;
	assert(fa != nullptr);
	
	auto *fb = (struct frag_bench *)malloc(sizeof(struct frag_bench));
	if (fb == nullptr) {
		perror("malloc");
		return -1;
	}
	
	fb->pa = (struct prog_args *)args->opts;
	
	size_t n_ops_total = args->n_ops_per_thread * args->n_threads;
	assert(n_ops_total != 0);

	fb->n_ops = args->n_ops_per_thread;

	///* Create pmemobj pool. */
	if (fb->pa->max_obj_size < ALLOC_MIN_SIZE)
		fb->pa->max_obj_size = ALLOC_MIN_SIZE;

	///* For data objects */
	size_t n_objs = args->n_ops_per_thread * args->n_threads;
	fb->poolsize = n_objs * (fb->pa->max_obj_size * fb->pa->peak_multiplier + OOB_HEADER_SIZE);

	///* multiply by FACTOR for metadata, fragmentation, etc. */
	fb->poolsize = fb->poolsize * FACTOR;

	if (args->is_poolset || util_file_is_device_dax(args->fname)) {
		if (args->fsize < fb->poolsize) {
			fprintf(stderr, "file size too large\n");
			goto free_fb;
		}
		fb->poolsize = 0;
	}
	else if (fb->poolsize < PMEMOBJ_MIN_POOL) {
		fb->poolsize = PMEMOBJ_MIN_POOL;
	}

	fb->poolsize = PAGE_ALIGNED_UP_SIZE(fb->poolsize);

	fb->pop = pmemobj_create(args->fname, nullptr, fb->poolsize, args->fmode);
	if (fb->pop == nullptr) {
		fprintf(stderr, "%s\n", pmemobj_errormsg());
		goto free_fb;
	}
	
	fb->pmemobj_array = (struct frag_obj **)malloc(sizeof(struct frag_obj*) * n_objs);
	if (fb->pmemobj_array == nullptr)
	{
		perror("malloc");
		goto free_pop;
	}
	for (unsigned i = 0; i < n_objs; ++i)
	{
		fb->pmemobj_array[i]= (struct frag_obj *)malloc(sizeof(struct frag_obj));

		if (fb->pmemobj_array[i] == nullptr)
		{
			perror("malloc");
			fb->free_pmemobj_array(i - 1);
			goto free_pop;
		}
		fb->pmemobj_array[i]->init(fb->pa);
	}

	fb->background_mem_usage = parse_memory_usage_type(fb->pa->background_mem_usage_type_str);
	scenario_index = parse_scenario(fb->pa->scenario);
	if (scenario_index == -1) {
		perror("wrong scenario name");
		goto free_pop;
	}
	fb->func_op = scenarios[scenario_index].func_op;
	fb->theoretical_memory_usage = 0;

	pmembench_set_priv(bench, fb);
	fragmentation = 0;
	return 0;

free_pop:
	pmemobj_close(fb->pop);
free_fb:
	free(fb);
	return -1;
}

/*
 * frag_exit -- function for de-initialization benchmark
 */
static int
frag_exit(struct benchmark *bench, struct benchmark_args *args)
{
	auto *fb = (struct frag_bench *)pmembench_get_priv(bench);
	std::cout << "frag exit\n";//todelete

	unsigned n_ops = args->n_ops_per_thread *args->n_threads;
	PMEMoid oid;
	size_t remaining = 0;
	size_t chunk = 100; /* calc at chunk level */
	while (pmemobj_alloc(fb->pop, &oid, chunk, 0, NULL, NULL) == 0)
		remaining += pmemobj_alloc_usable_size(oid) + 16;

	size_t allocated_sum = 0;
	size_t allocated_sum2 = 0;
	oid = pmemobj_root(fb->pop, 1);
	for (size_t n = 0; n < n_ops; ++n) {
		if (fb->pmemobj_array[n]->is_allocated == false)
			continue;
		oid = fb->pmemobj_array[n]->oid;
		oid.pool_uuid_lo = fb->pmemobj_array[n]->oid.pool_uuid_lo;
		allocated_sum += pmemobj_alloc_usable_size(oid);// +16;
		allocated_sum2 += pmemobj_alloc_usable_size(oid) +16;
	}

	size_t used = fb->poolsize - remaining;

	fragmentation = ((float)used / fb->theoretical_memory_usage) - 1.f;
	fragmentation2 = ((float)used / allocated_sum) - 1.f;
	float fragmentation3 = ((float)used / allocated_sum2) - 1.f;
	std::cout << "fragmentation " << fragmentation << std::endl;//todelete
	std::cout << "fragmentation2 " << fragmentation2 << std::endl;//todelete
	std::cout << "fragmentation3 " << fragmentation3 << std::endl;//todelete
	fb->free_pmemobj_array(n_ops);

	pmemobj_close(fb->pop);

	free(fb);
	std::cout << "free fb\n";//todelete
	return 0;
}
static void
frag_print_fragmentation(struct benchmark *bench,
	struct benchmark_args *args,
	struct total_results *res)
{
	printf(";%f;%f", fragmentation,fragmentation2);
}

static struct benchmark_clo frag_clo[9];
static struct benchmark_info test_info;

CONSTRUCTOR(frag_constructor)
void
frag_constructor(void)
{
	frag_clo[0].opt_long = "background-memory-usage";
	frag_clo[0].descr = "Tested memory usage pattern";
	frag_clo[0].type = CLO_TYPE_STR;
	frag_clo[0].off = 
		clo_field_offset(struct prog_args, background_mem_usage_type_str);
	frag_clo[0].def = "flat";
	frag_clo[0].ignore_in_res = false;
	
	frag_clo[1].opt_long = "start-obj-size";
	frag_clo[1].descr = "start obj size";
	frag_clo[1].type = CLO_TYPE_UINT;
	frag_clo[1].off = clo_field_offset(struct prog_args, start_obj_size);
	frag_clo[1].def = "64";
	frag_clo[1].type_uint.size = clo_field_size(struct prog_args, start_obj_size);
	frag_clo[1].type_uint.base = CLO_INT_BASE_DEC;
	frag_clo[1].type_uint.min = 0;
	frag_clo[1].type_uint.max = ~0;

	frag_clo[2].opt_long = "max-obj-size";
	frag_clo[2].descr = "maximum obj size";
	frag_clo[2].type = CLO_TYPE_UINT;
	frag_clo[2].off = clo_field_offset(struct prog_args, max_obj_size);
	frag_clo[2].def = "1024";
	frag_clo[2].type_uint.size = clo_field_size(struct prog_args, max_obj_size);
	frag_clo[2].type_uint.base = CLO_INT_BASE_DEC;
	frag_clo[2].type_uint.min = 0;
	frag_clo[2].type_uint.max = ~0;

	frag_clo[3].opt_long = "lifetime";
	frag_clo[3].descr = "objects lifetime in s";
	frag_clo[3].type = CLO_TYPE_UINT;
	frag_clo[3].off = clo_field_offset(struct prog_args, lifetime);
	frag_clo[3].def = "1";
	frag_clo[3].type_uint.size = clo_field_size(struct prog_args, lifetime);
	frag_clo[3].type_uint.base = CLO_INT_BASE_DEC;
	frag_clo[3].type_uint.min = 0;
	frag_clo[3].type_uint.max = ~0;

	frag_clo[4].opt_long = "peak-lifetime";
	frag_clo[4].descr = "objects memory peak lifetime in ms";
	frag_clo[4].type = CLO_TYPE_UINT;
	frag_clo[4].off = clo_field_offset(struct prog_args, peak_lifetime);
	frag_clo[4].def = "10";
	frag_clo[4].type_uint.size = clo_field_size(struct prog_args, peak_lifetime);
	frag_clo[4].type_uint.base = CLO_INT_BASE_DEC;
	frag_clo[4].type_uint.min = 0;
	frag_clo[4].type_uint.max = ~0;

	frag_clo[5].opt_long = "growth-factor";
	frag_clo[5].descr = "factor how much object should growth";
	frag_clo[5].type = CLO_TYPE_UINT;
	frag_clo[5].off = clo_field_offset(struct prog_args, growth_factor);
	frag_clo[5].def = "8";
	frag_clo[5].type_uint.size = clo_field_size(struct prog_args, growth_factor);
	frag_clo[5].type_uint.base = CLO_INT_BASE_DEC;
	frag_clo[5].type_uint.min = 0;
	frag_clo[5].type_uint.max = ~0;

	frag_clo[6].opt_long = "peak-multiplier";
	frag_clo[6].descr = "multiplier for peak memory usage growth";
	frag_clo[6].type = CLO_TYPE_UINT;
	frag_clo[6].off = clo_field_offset(struct prog_args, peak_multiplier);
	frag_clo[6].def = "10";
	frag_clo[6].type_uint.size = clo_field_size(struct prog_args, peak_multiplier);
	frag_clo[6].type_uint.base = CLO_INT_BASE_DEC;
	frag_clo[6].type_uint.min = 0;
	frag_clo[6].type_uint.max = ~0;

	frag_clo[7].opt_long = "peak-allocs";
	frag_clo[7].descr = "number of small allocations on peak operation";
	frag_clo[7].type = CLO_TYPE_UINT;
	frag_clo[7].off = clo_field_offset(struct prog_args, peak_allocs);
	frag_clo[7].def = "100";
	frag_clo[7].type_uint.size = clo_field_size(struct prog_args, peak_allocs);
	frag_clo[7].type_uint.base = CLO_INT_BASE_DEC;
	frag_clo[7].type_uint.min = 0;
	frag_clo[7].type_uint.max = ~0;

	frag_clo[8].opt_long = "scenario";
	frag_clo[8].descr = "test scenario";
	frag_clo[8].type = CLO_TYPE_STR;
	frag_clo[8].off = clo_field_offset(struct prog_args, scenario);
	frag_clo[8].def = "basic";

	test_info.name = "test_frag";
	test_info.brief = "Benchmark test_frag operation";
	test_info.init = frag_init; 
	test_info.exit = frag_exit;
	test_info.multithread = true;
	test_info.multiops = true;
	test_info.init_worker = frag_init_worker;
	test_info.free_worker = frag_free_worker;
	test_info.operation = frag_operation;
	test_info.print_extra_values = frag_print_fragmentation;
	test_info.clos = frag_clo;
	test_info.nclos = ARRAY_SIZE(frag_clo);
	test_info.opts_size = sizeof(struct prog_args);
	test_info.rm_file = true;
	test_info.allow_poolset = true;

	REGISTER_BENCHMARK(test_info);
}

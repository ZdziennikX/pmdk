/*
 * Copyright 2015-2018, Intel Corporation
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
	MEM_USAGE_RAMP,
};

/*
 * prog_args -- benchmark specific arguments
 */
struct prog_args {
	char *mem_usage_type_str; /* mem_usage_type: flat, peak, ramp */
	size_t min_obj_size;
	size_t max_obj_size;
	int nb_memory_blocks;
	unsigned lifetime;
	
	//	size_t fsize; /* requested file size */
	//	bool no_warmup; /* don't do warmup */
	//	unsigned seed; /* seed for randomization */
	//	char *type_str; /* type: blk, file, memcpy */
	//	char *mode_str; /* mode: stat, seq, rand */
};

struct frag_bench
{
	PMEMobjpool *pop; /* persistent pool handle */
	struct prog_args *pa; /* prog_args structure */
	mem_usage_type mem_usage;
	//frag_obj *frag_objs;usunac jesli zostanie w workerze
	uint64_t nobjs;
};

struct frag_obj
{
	int block_lifetime;
	size_t block_size;
	PMEMoid oid;
	bool is_allocated;
	//void * ptr;

	void init(struct prog_args *args)
	{
		block_lifetime = args->lifetime;
		block_size = args->min_obj_size;
		is_allocated = false;
	}
};

/*
* struct frag_worker -- fragmentation worker context
*/
struct frag_worker {
	frag_obj *pmemobj;
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

//static int op_count = 0;

/*
 * frag_operation -- main operations for fragmentation benchmark
 */
static int
frag_operation(struct benchmark *bench, struct operation_info *info)
{

	
	auto *fb = (struct frag_bench *)pmembench_get_priv(bench);
	auto *fworker = (struct frag_worker *)info->worker->priv;

	
	std::cout << "frag_operation info index" << info->index << " worker index " << info->worker->index << " ";   //to delete
	//std::cout << " index w tablicy " << fworker->frag_obj_start_offset+info->index;
	std::cout << fb->pa->mem_usage_type_str << "\n";
		//auto *bworker = (struct frag_worker *)info->worker->priv;
	
	void *ptr;
	if (pmemobj_alloc(fb->pop, &fworker->pmemobj->oid, fworker->pmemobj->block_size, 0, nullptr,
		nullptr)) {
		perror("pmemobj_alloc");
		free(fworker->pmemobj);
		free(fworker);
		return -1;
	}
	//os_off_t off = bworker->blocks[info->index];
	//return bb->worker(bb, info->args, bworker, off);
	return 0;
}

/*
 * frag_init_worker -- init benchmark worker
 */
static int
frag_init_worker(struct benchmark *bench,
	struct benchmark_args *args,
	struct worker_info *worker)
{
	struct frag_worker *fworker =
		(struct frag_worker *)malloc(sizeof(*fworker));

	if (!fworker) {
		perror("malloc");
		return -1;
	}

	fworker->pmemobj = (struct frag_obj *)malloc(sizeof(struct frag_obj));
	if (fworker->pmemobj == nullptr)
	{
		perror("malloc");
		goto free_fworker;
	}

	auto *fb = (struct frag_bench *)pmembench_get_priv(bench);

	//fworker->pmemobj->block_lifetime = fb->pa->lifetime;
	fworker->pmemobj = (struct frag_obj *)malloc(sizeof(struct frag_obj));
	if (fworker->pmemobj == nullptr)
	{
		perror("malloc");
		goto free_fworker;
	}

	fworker->pmemobj->init(fb->pa);
	//fworker->frag_obj_start_offset = worker->index * args->n_ops_per_thread;
	
	worker->priv = fworker;
	return 0;

free_fworker:
	free(fworker);
	return -1;
}

/*
 * frag_free_worker -- cleanup benchmark worker
 */
static void
frag_free_worker(struct benchmark *bench,
	struct benchmark_args *args,
	struct worker_info *worker)
{
	std::cout << "free worker index " << worker->index << " opinfoindex " << worker->opinfo->index << std::endl;
	auto *fworker = (struct frag_worker *)worker->priv;
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
	
	auto *fa = (struct prog_args *)args->opts;
	assert(fa != nullptr);
	
	auto *fb = (struct frag_bench *)malloc(sizeof(struct frag_bench));
	if (fb == nullptr) {
		perror("malloc");
		return -1;
	}
	
	fb->pa = (struct prog_args *)args->opts;
	
	size_t poolsize;
	size_t n_ops_total = args->n_ops_per_thread * args->n_threads;
	assert(n_ops_total != 0);

	fb->nobjs = args->n_ops_per_thread * args->n_threads;
	//fb->nobjs = args->n_threads;

	///* Create pmemobj pool. */
	if (fb->pa->max_obj_size < ALLOC_MIN_SIZE)
		fb->pa->max_obj_size = ALLOC_MIN_SIZE;

	///* For data objects */
	poolsize = fb->nobjs * (fb->pa->max_obj_size + OOB_HEADER_SIZE);

	///* multiply by FACTOR for metadata, fragmentation, etc. */
	poolsize = poolsize * FACTOR;

	if (args->is_poolset || util_file_is_device_dax(args->fname)) {
		if (args->fsize < poolsize) {
			fprintf(stderr, "file size too large\n");
			goto free_fb;
		}
		poolsize = 0;
	}
	else if (poolsize < PMEMOBJ_MIN_POOL) {
		poolsize = PMEMOBJ_MIN_POOL;
	}

	poolsize = PAGE_ALIGNED_UP_SIZE(poolsize);

	fb->pop = pmemobj_create(args->fname, nullptr, poolsize, args->fmode);
	if (fb->pop == nullptr) {
		fprintf(stderr, "%s\n", pmemobj_errormsg());
		goto free_fb;
	}
	
	fb->mem_usage = parse_memory_usage_type(fb->pa->mem_usage_type_str);
		
	pmembench_set_priv(bench, fb);

	return 0;

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
	//todo free oids
	pmemobj_close(fb->pop);

	free(fb);
	return 0;
}

static struct benchmark_clo frag_clo[4];
static struct benchmark_info test_info;

CONSTRUCTOR(frag_constructor)
void
frag_constructor(void)
{
	frag_clo[0].opt_long = "memory-usage";
	frag_clo[0].descr = "Tested memory usage pattern";
	frag_clo[0].type = CLO_TYPE_STR;
	frag_clo[0].off = 
		clo_field_offset(struct prog_args, mem_usage_type_str);
	frag_clo[0].def = "flat";
	frag_clo[0].ignore_in_res = false;
	
	frag_clo[1].opt_long = "min-obj-size";
	frag_clo[1].descr = "minimum obj size";
	frag_clo[1].type = CLO_TYPE_UINT;
	frag_clo[1].off = clo_field_offset(struct prog_args, min_obj_size);
	frag_clo[1].def = "64";
	frag_clo[1].type_uint.size = clo_field_size(struct prog_args, min_obj_size);
	frag_clo[1].type_uint.base = CLO_INT_BASE_DEC;
	frag_clo[1].type_uint.min = 0;
	frag_clo[1].type_uint.max = ~0;

	frag_clo[2].opt_long = "max-obj-size";
	frag_clo[2].descr = "maximum obj size";
	frag_clo[2].type = CLO_TYPE_UINT;
	frag_clo[2].off = clo_field_offset(struct prog_args, max_obj_size);
	frag_clo[2].def = "64";
	frag_clo[2].type_uint.size = clo_field_size(struct prog_args, max_obj_size);
	frag_clo[2].type_uint.base = CLO_INT_BASE_DEC;
	frag_clo[2].type_uint.min = 0;
	frag_clo[2].type_uint.max = ~0;

	frag_clo[3].opt_long = "lifetime";
	frag_clo[3].descr = "objects lifetime in ms";
	frag_clo[3].type = CLO_TYPE_UINT;
	frag_clo[3].off = clo_field_offset(struct prog_args, max_obj_size);
	frag_clo[3].def = "1000";
	frag_clo[3].type_uint.size = clo_field_size(struct prog_args, max_obj_size);
	frag_clo[3].type_uint.base = CLO_INT_BASE_DEC;
	frag_clo[3].type_uint.min = 0;
	frag_clo[3].type_uint.max = ~0;
	/*frag_clo[1].opt_long = "nb_frag_objs";
	frag_clo[1].descr = "Number of memory blocks";
	frag_clo[1].off = clo_field_offset(struct prog_args, nb_memory_blocks);
	frag_clo[1].def = "1";
	frag_clo[1].type = CLO_TYPE_UINT;
	frag_clo[1].type_uint.size = clo_field_size(struct prog_args, nb_memory_blocks);
	frag_clo[1].type_uint.base = CLO_INT_BASE_DEC;
	frag_clo[1].type_uint.min = 1;
	frag_clo[1].type_uint.max = UINT_MAX;
	*/
	test_info.name = "test_frag";
	test_info.brief = "Benchmark test_frag operation";
	test_info.init = frag_init; 
	test_info.exit = frag_exit;
	test_info.multithread = true;
	test_info.multiops = true;
	test_info.init_worker = frag_init_worker;
	test_info.free_worker = frag_free_worker;
	test_info.operation = frag_operation;
	test_info.clos = frag_clo;
	test_info.nclos = ARRAY_SIZE(frag_clo);
	test_info.opts_size = sizeof(struct prog_args);
	test_info.rm_file = true;
	test_info.allow_poolset = true;

	REGISTER_BENCHMARK(test_info);
}
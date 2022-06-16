#include <thread_util.h>

static int core_thread_aff[36] = {-1};
static std::mutex aff_mutex;
static bool IS_ALL_JOB_PUSHED[NUM_THREADS] = {false};
static bool IS_ALL_JOB_DONE[NUM_THREADS] = {false};
gsj::thread_barrier syn_barrier(2);

namespace gsj
{

    // core_id = 0, 1, ... n-1, where n is the system's number of cores
    /*
#define _GNU_SOURCE          
#include <sched.h>

int sched_setaffinity(pid_t pid, size_t cpusetsize,
                      cpu_set_t *mask);

int sched_getaffinity(pid_t pid, size_t cpusetsize,
                      cpu_set_t *mask);

 */

    int stick_this_thread_to_core(int core_id)
    {
        int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
        if (core_id < 0 || core_id >= num_cores)
            return EINVAL;

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);

        //std::thread::id current_thread = std::this_thread::get_id();
        pid_t current_thread = getpid();
        return sched_setaffinity(current_thread, sizeof(cpu_set_t), &cpuset);
    }

    int get_victim_thread_id(int num_threads, int my_thread_id, int current_victim_id, bool enable_NUMA)
    {
        int victim_id, i;
        /* No NUMA, just send current_victim_id+1. Round Robin*/
        if (!enable_NUMA)
        {
            if (current_victim_id == INIT_VICTIM)
            {
                victim_id = (my_thread_id + 1) % num_threads;
                return victim_id;
            }

            victim_id = (current_victim_id + 1) % num_threads;

            if (victim_id == my_thread_id)
            {
                bool is_all_job_done_temp = true;
                for (i = 0; i < num_threads; ++i)
                {
                    if (!IS_ALL_JOB_DONE[i])
                        is_all_job_done_temp = false;
                }

                if (is_all_job_done_temp)
                    victim_id = NO_MORE_VICTIM;
                else
                    victim_id = (my_thread_id + 1) % num_threads;
            }

            return victim_id;
        }

        if (my_thread_id < 0 || my_thread_id > 35) /*error checking*/
            return NO_MORE_VICTIM;

        int my_numa_node;
        my_numa_node = core_thread_aff[my_thread_id];
        victim_id = my_numa_node;
        /* Using NUMA. On bebop, Core 0-17 on numa node 0; Core 18-35 on numa node 1*/
        /* First check local numa node*/
        for (i = 0; i < num_threads; ++i)
        {
            victim_id++;
            victim_id = victim_id % num_threads;

            if (core_thread_aff[victim_id] != my_numa_node)
                continue;

            if (IS_ALL_JOB_DONE[victim_id])
                continue;

            return victim_id;
        }

        /* Then check another numa node*/
        for (i = 0; i < num_threads; ++i)
        {
            victim_id++;
            victim_id = victim_id % num_threads;

            if (core_thread_aff[victim_id] == my_numa_node)
                continue;

            if (IS_ALL_JOB_DONE[victim_id])
                continue;

            return victim_id;
        }

        /* All jobs are done */
        return NO_MORE_VICTIM;
    }

    void Thief_Thread_spatial_join(WorkStealingQueue<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> *queue)
    {
        uint counter = 0;

        spdlog::debug("Enter function {}", "Thief_Thread_spatial_join");
        double t_total, t_steal, t_join;
        t_total = t_steal = t_join = 0.0;
        auto t_begin = std::chrono::steady_clock::now();

        if (queue == NULL)
        {
            return;
        }

        while (1)
        {
            auto t_steal_begin = std::chrono::steady_clock::now();

            if (queue->empty())
                break;

            std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair_geoms = NULL;

            try
            {
                std::optional<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> item = queue->steal();
                temp_pair_geoms = item.value();
            }
            catch (std::bad_optional_access &e)
            {
                continue;
            }

            auto t_steal_end = std::chrono::steady_clock::now();

            for (std::vector<geos::geom::Geometry *>::iterator void_itr = temp_pair_geoms->second->begin();
                 void_itr != temp_pair_geoms->second->end(); ++void_itr)
            {
                geos::geom::Geometry *qrd_geom = *void_itr;
                try
                {
                    std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);
                    counter += geoms_intersection->getNumPoints();
                }
                catch (std::exception &e)
                {
                    //Do nothing, just skip the exception
                }
            }

            auto t_join_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> t_diff_temp = t_steal_end - t_steal_begin;
            t_steal += t_diff_temp.count();
            t_diff_temp = t_join_end - t_steal_end;
            t_join += t_diff_temp.count();
        }

        auto t_end = std::chrono::steady_clock::now();
        std::chrono::duration<double> t_diff_temp = t_end - t_begin;
        t_total = t_diff_temp.count();

        spdlog::info("Theif thread: result, {0:d}; total time {1:03.3f}; steal time {2:03.3f}; join time {3:03.3f}",
                     counter, t_total, t_steal, t_join);

        spdlog::debug("Leave function {}", "Thief_Thread_spatial_join");
    }

    void Owner_Thread_spatial_join(WorkStealingQueue<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> *queue)
    {
        uint counter = 0;

        spdlog::debug("Enter function {}", "Owner_Thread_spatial_join");
        double t_total, t_steal, t_join;
        t_total = t_steal = t_join = 0.0;
        auto t_begin = std::chrono::steady_clock::now();

        if (queue == NULL)
        {
            return;
        }

        while (1)
        {
            auto t_steal_begin = std::chrono::steady_clock::now();

            if (queue->empty())
                break;

            std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair_geoms = NULL;

            try
            {
                std::optional<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> item = queue->pop();
                temp_pair_geoms = item.value();
            }
            catch (std::bad_optional_access &e)
            {
                continue;
            }

            auto t_steal_end = std::chrono::steady_clock::now();

            for (std::vector<geos::geom::Geometry *>::iterator void_itr = temp_pair_geoms->second->begin();
                 void_itr != temp_pair_geoms->second->end(); ++void_itr)
            {
                geos::geom::Geometry *qrd_geom = *void_itr;
                try
                {
                    std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);
                    counter += geoms_intersection->getNumPoints();
                }
                catch (std::exception &e)
                {
                    //Do nothing, just skip the exception
                }
            }

            auto t_join_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> t_diff_temp = t_steal_end - t_steal_begin;
            t_steal += t_diff_temp.count();
            t_diff_temp = t_join_end - t_steal_end;
            t_join += t_diff_temp.count();
        }

        auto t_end = std::chrono::steady_clock::now();
        std::chrono::duration<double> t_diff_temp = t_end - t_begin;
        t_total = t_diff_temp.count();

        spdlog::info("Owner thread: result, {0:d}; total time {1:03.3f}; pop time {2:03.3f}; join time {3:03.3f}",
                     counter, t_total, t_steal, t_join);

        spdlog::debug("Leave function {}", "Owner_Thread_spatial_join");
    }

    void Thread_MPI_spatial_join_pre_parsing(std::list<std::pair<geos::geom::Geometry *, int> *> *list_geoms_1, std::list<std::pair<geos::geom::Geometry *, int> *> *list_geoms_2,
                                             int thread_rank, int num_threads, WorkStealingQueue<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> **queue,
                                             volatile int *is_queue_filled, ulong *join_results)
    {
        ulong local_join_result = 0;
        int numa_node, current_cpu_id;

        spdlog::debug("Enter function {}", "Thread_Spatial_join_multi_owners_parsing");

        double t_total, t_parse, t_steal, t_join, t_mpi;
        std::chrono::duration<double> t_diff_temp;
        t_total = t_parse = t_steal = t_join = t_mpi = 0.0;
        auto t_begin = std::chrono::steady_clock::now();

        current_cpu_id = sched_getcpu();

        numa_node = 0;

#ifdef NUMA_ENABLED
        numa_node = numa_node_of_cpu(current_cpu_id);
#endif //ifdef NUMA_ENABLED

        aff_mutex.lock();
        core_thread_aff[thread_rank] = numa_node;
        aff_mutex.unlock();

        syn_barrier.wait();

        auto t_parse_end = std::chrono::steady_clock::now();
        t_diff_temp = t_parse_end - t_begin;

        if (list_geoms_1 == NULL || list_geoms_2 == NULL)
        {
            spdlog::error("NULL geoms lists");
            return;
        }
        /************************Build index************************************************************************/
        geos::index::strtree::STRtree index_for_layer_1;
        for (std::list<std::pair<geos::geom::Geometry *, int> *>::iterator itr = list_geoms_1->begin(); itr != list_geoms_1->end(); ++itr)
        {
            std::pair<geos::geom::Geometry *, int> *temp_geom = *itr;

            index_for_layer_1.insert(temp_geom->first->getEnvelopeInternal(), temp_geom);
        }

        auto t_index_end = std::chrono::steady_clock::now();
        t_diff_temp = t_index_end - t_parse_end;

        spdlog::debug("Thread [{0:d}] index built, time {1:03.3f}", thread_rank, t_diff_temp.count());

        /************************Build index end************************************************************************/

        for (std::list<std::pair<geos::geom::Geometry *, int> *>::iterator itr = list_geoms_2->begin(); itr != list_geoms_2->end(); ++itr)
        {
            std::pair<geos::geom::Geometry *, int> *temp_geom = *itr;
            std::vector<void *> results;

            const geos::geom::Envelope *temp_env = temp_geom->first->getEnvelopeInternal();

            index_for_layer_1.query(temp_env, results);

            if (results.size() != 0)
            {
                std::vector<geos::geom::Geometry *> *temp_geom_vect = new std::vector<geos::geom::Geometry *>();

                for (std::vector<void *>::iterator void_itr = results.begin(); void_itr != results.end(); void_itr++)
                {
                    void *temp_geom_ptr = *void_itr;
                    std::pair<geos::geom::Geometry *, int> *qrd_geom = (std::pair<geos::geom::Geometry *, int> *)temp_geom_ptr;

                    if (qrd_geom->second == temp_geom->second)
                        temp_geom_vect->push_back(qrd_geom->first);
                }

                if (temp_geom_vect->size() <= TASKS_PER_JOB)
                {
                    std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair =
                        new std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *>;

                    temp_pair->first = temp_geom->first;
                    temp_pair->second = temp_geom_vect;

                    queue[thread_rank]->push(temp_pair);
                }
                else
                {
                    uint i, j;
                    for (i = 0; i < temp_geom_vect->size();)
                    {
                        std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair =
                            new std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *>;

                        std::vector<geos::geom::Geometry *> *temp_geom_vect_limited_size = new std::vector<geos::geom::Geometry *>();

                        for (j = 0; j < TASKS_PER_JOB; ++j)
                        {
                            if (i + j < temp_geom_vect->size())
                            {
                                temp_geom_vect_limited_size->push_back(temp_geom_vect->at(i + j));
                            }
                            else
                            {
                                break;
                            }
                        }

                        if (temp_geom_vect_limited_size->empty())
                        {
                            break;
                        }
                        else
                        {
                            temp_pair->first = temp_geom->first;
                            temp_pair->second = temp_geom_vect_limited_size;
                            queue[thread_rank]->push(temp_pair);
                        }
                        i += j;
                    }
                }
            }
        }

        IS_ALL_JOB_PUSHED[thread_rank] = true;

        auto t_push_end = std::chrono::steady_clock::now();
        t_diff_temp = t_push_end - t_index_end;
        spdlog::debug("Thread [{0:d}] all jobs pushed, time {1:03.3f}", thread_rank, t_diff_temp.count());

        bool is_all_job_pushed_temp = true;

        while (1)
        {

            if (thread_rank == 0  && is_queue_filled[num_threads - 1] == 3 )
            {
                is_all_job_pushed_temp = true;
                for (int i = 0; i < num_threads; ++i)
                {
                    if (!IS_ALL_JOB_PUSHED[i])
                        is_all_job_pushed_temp = false;
                }

                if (is_all_job_pushed_temp)
                    is_queue_filled[num_threads - 1] = 2;
            }

            auto t_pop_begin = std::chrono::steady_clock::now();

            if (queue[thread_rank]->empty())
                break;

            std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair_geoms = NULL;

            try
            {
                std::optional<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> item = queue[thread_rank]->pop();
                temp_pair_geoms = item.value();
            }
            catch (std::bad_optional_access &e)
            {
                continue;
            }

            auto t_pop_end = std::chrono::steady_clock::now();

#ifdef USE_ST_INTERSECTS
            std::unique_ptr<geos::geom::prep::PreparedGeometry> pg =
                geos::geom::prep::PreparedGeometryFactory::prepare(temp_pair_geoms->first);
#endif //ifdef USE_ST_INTERSECTS

            for (std::vector<geos::geom::Geometry *>::iterator void_itr = temp_pair_geoms->second->begin();
                 void_itr != temp_pair_geoms->second->end(); void_itr++)
            {
                geos::geom::Geometry *qrd_geom = *void_itr;
                try
                {
#ifdef USE_ST_INTERSECTION
                    std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);
                    local_join_result += geoms_intersection->getNumPoints();
                    //if (geoms_intersection->getNumPoints() != 0)
                    //    ++local_join_result;

                    geoms_intersection = nullptr;
#elif USE_ST_INTERSECTS
                    if (pg->intersects(qrd_geom))
                        ++local_join_result;

#elif USE_ST_UNION
                    std::unique_ptr<geos::geom::Geometry> geoms_union = temp_pair_geoms->first->Union(qrd_geom);

                    if (geoms_union != nullptr)
                    {
                        local_join_result += geoms_union->getNumPoints();
                        geoms_union = nullptr;
                    }

#else //default using USE_ST_INTERSECTION
                    std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);
                    local_join_result += geoms_intersection->getNumPoints();
                    //if (geoms_intersection->getNumPoints() != 0)
                    //    ++local_join_result;

                    geoms_intersection = nullptr;
#endif
                }
                catch (std::exception &e)
                {
                }
            }

            if (temp_pair_geoms)
                delete temp_pair_geoms;

            auto t_join_end = std::chrono::steady_clock::now();

            t_diff_temp = t_pop_end - t_pop_begin;
            t_steal += t_diff_temp.count();

            t_diff_temp = t_join_end - t_pop_end;
            t_join += t_diff_temp.count();
        }

        IS_ALL_JOB_DONE[thread_rank] = true;

        //int thread_counter = (thread_rank+1) % num_threads;
        int thread_counter = get_victim_thread_id(num_threads, thread_rank, INIT_VICTIM, true);
        while (thread_counter != NO_MORE_VICTIM)
        {
            auto t_steal_begin = std::chrono::steady_clock::now();

            if (queue[thread_counter]->empty())
            {

                thread_counter = get_victim_thread_id(num_threads, thread_rank, thread_counter, true);
                continue;
            }

            if (!IS_ALL_JOB_PUSHED[thread_counter])
            {
                continue;
            }

            if (thread_rank == 0 && is_queue_filled[num_threads - 1] == 3)
            {
                is_all_job_pushed_temp = true;
                for (int i = 0; i < num_threads; ++i)
                {
                    if (!IS_ALL_JOB_PUSHED[i])
                        is_all_job_pushed_temp = false;
                }

                if (is_all_job_pushed_temp )
                    is_queue_filled[num_threads - 1] = 2;
            }

            std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair_geoms = NULL;
            try
            {
                std::optional<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> item = queue[thread_counter]->steal();
                temp_pair_geoms = item.value();
            }
            catch (std::bad_optional_access &e)
            {
                continue;
            }

            auto t_steal_end = std::chrono::steady_clock::now();

#ifdef USE_ST_INTERSECTS
            std::unique_ptr<geos::geom::prep::PreparedGeometry> pg =
                geos::geom::prep::PreparedGeometryFactory::prepare(temp_pair_geoms->first);
#endif //ifdef USE_ST_INTERSECTS

            for (std::vector<geos::geom::Geometry *>::iterator void_itr = temp_pair_geoms->second->begin();
                 void_itr != temp_pair_geoms->second->end(); ++void_itr)
            {
                geos::geom::Geometry *qrd_geom = *void_itr;
                try
                {
#ifdef USE_ST_INTERSECTION
                    std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);
                    local_join_result += geoms_intersection->getNumPoints();
                    //if (geoms_intersection->getNumPoints() != 0)
                    //    ++local_join_result;

                    geoms_intersection = nullptr;
#elif USE_ST_INTERSECTS
                    if (pg->intersects(qrd_geom))
                        ++local_join_result;

#elif USE_ST_UNION
                    std::unique_ptr<geos::geom::Geometry> geoms_union = temp_pair_geoms->first->Union(qrd_geom);

                    if (geoms_union != nullptr)
                    {
                        local_join_result += geoms_union->getNumPoints();
                        geoms_union = nullptr;
                    }

#else //default using USE_ST_INTERSECTION
                    std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);
                    local_join_result += geoms_intersection->getNumPoints();
                    //if (geoms_intersection->getNumPoints() != 0)
                    //    ++local_join_result;

                    geoms_intersection = nullptr;
#endif
                }
                catch (std::exception &e)
                {
                }
            }

            if (temp_pair_geoms)
                delete temp_pair_geoms;

            auto t_join_end = std::chrono::steady_clock::now();

            t_diff_temp = t_steal_end - t_steal_begin;
            t_steal += t_diff_temp.count();

            t_diff_temp = t_join_end - t_steal_end;
            t_join += t_diff_temp.count();
        }

        auto t_join_end = std::chrono::steady_clock::now();

        /*
        if (thread_rank == 0 && !is_all_job_pushed_temp)
        {
            is_all_job_pushed_temp = true;
            is_queue_filled[num_threads - 1] = 2;
        }*/

        if (thread_rank == 0)
        {
            if (is_queue_filled[num_threads - 1] == 3)
                is_queue_filled[num_threads - 1] = 2;
            spdlog::debug("All local jobs are done");
        }

        bool is_local_list_freed = false;

        // Now waiting for jobs from other
        int queue_counter = 0;

        while (1)
        {
            /*
            queue_counter = -1;
            for (int i = 0; i < num_threads; ++i)
            {
                if (is_queue_filled[queue_counter] != 0)
                {
                    queue_counter = i;
                    break;
                }
            }

            queue_counter = queue_counter % num_threads;

            //spdlog::info("T{}, visit q{}, {}",thread_rank, queue_counter, is_queue_filled[queue_counter]);
            if (queue_counter == -1 || queue[queue_counter]->empty())
            {

                
                if (thread_rank == 0)
                {
                    spdlog::debug("Failed to get a queue");
                    for (int i = 0; i < num_threads; ++i)
                        std::cout<<is_queue_filled[queue_counter]<<" ";

                    std::cout<<std::endl;
                }
                
                std::this_thread::sleep_for(std::chrono::nanoseconds(100));
                continue;
            }
            */
            //spdlog::info("{} {}",queue_counter, is_queue_filled[queue_counter]);

            
            auto t_temp_start = std::chrono::steady_clock::now();

            queue_counter = queue_counter%num_threads;
            if (is_queue_filled[queue_counter] == 0 && queue[(queue_counter+1)%num_threads]->empty())
            {
                //queue_counter = (queue_counter+1)% num_threads;
                std::this_thread::sleep_for(std::chrono::nanoseconds(50));

                if (thread_rank == 0)
                {
                    auto t_temp_end = std::chrono::steady_clock::now();

                    std::chrono::duration<double> t_diff_temp = t_temp_end - t_temp_start;

                    if (t_diff_temp.count() > 1.0)
                    {
                        spdlog::debug("T0 waiting on q {}", queue_counter);

                        for (int i = 0; i < num_threads; ++i)
                            std::cout << is_queue_filled[i] << " ";

                        std::cout << std::endl;

                        t_temp_start = std::chrono::steady_clock::now();
                    }
                }
            }

            if (is_queue_filled[queue_counter] == -1)
                break;

            if (is_queue_filled[queue_counter] == 2 || queue[queue_counter]->empty())
            {
                /*
                if (thread_rank == 0)
                {
                    spdlog::debug("T0 got a cycle {}", queue_counter);

                    for (int i = 0; i < num_threads; ++i)
                        std::cout << is_queue_filled[i] << " ";

                    std::cout << std::endl;

                    is_queue_filled[queue_counter] = 0;
                }
                */
                queue_counter++;
                continue;
            }

            
            
            //syn_barrier.wait();

            /*
            if (vect_atomic_counter->at(queue_counter) != 0)
            {
                (vect_atomic_counter->at(queue_counter))++;

                spdlog::debug("PRE rank {}, q {} , {} {} ", thread_rank, queue_counter, is_queue_filled[queue_counter], vect_atomic_counter->at(queue_counter) );
                if (vect_atomic_counter->at(queue_counter) >= num_threads)
                {
                    vect_atomic_counter->at(queue_counter) = 0;
                    is_queue_filled[queue_counter] = 0;
                }

                queue_counter++;
                continue;
            }*/
            //spdlog::info("{} {}",queue_counter, is_queue_filled[queue_counter]);

            while (!queue[queue_counter]->empty())
            {
                std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair_geoms = NULL;
                try
                {
                    std::optional<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> item = queue[queue_counter]->steal();
                    temp_pair_geoms = item.value();
                }
                catch (std::bad_optional_access &e)
                {
                    continue;
                }

#ifdef USE_ST_INTERSECTS
                std::unique_ptr<geos::geom::prep::PreparedGeometry> pg =
                    geos::geom::prep::PreparedGeometryFactory::prepare(temp_pair_geoms->first);
#endif //ifdef USE_ST_INTERSECTS

                for (std::vector<geos::geom::Geometry *>::iterator void_itr = temp_pair_geoms->second->begin();
                     void_itr != temp_pair_geoms->second->end(); ++void_itr)
                {
                    geos::geom::Geometry *qrd_geom = *void_itr;
                    try
                    {
#ifdef USE_ST_INTERSECTION
                        std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);
                        local_join_result += geoms_intersection->getNumPoints();
                        //if (geoms_intersection->getNumPoints() != 0)
                        //    ++local_join_result;

                        geoms_intersection = nullptr;

#elif USE_ST_INTERSECTS
                        if (pg->intersects(qrd_geom))
                            ++local_join_result;

#elif USE_ST_UNION
                        std::unique_ptr<geos::geom::Geometry> geoms_union = temp_pair_geoms->first->Union(qrd_geom);

                        if (geoms_union != nullptr)
                        {
                            local_join_result += geoms_union->getNumPoints();
                            geoms_union = nullptr;
                        }

#else //default using USE_ST_INTERSECTION
                        std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);

                        local_join_result += geoms_intersection->getNumPoints();
                        //if (geoms_intersection->getNumPoints() != 0)
                        //    ++local_join_result;

                        geoms_intersection = nullptr;
#endif
                        //qrd_geom->getCoordinates() = nullptr;
                        //qrd_geom->getCoordinates().reset();
                        //delete qrd_geom;
                        //qrd_geom->~Geometry();
                        //geos::geom::CoordinateSequence *temp_ptr_to_free = qrd_geom->getCoordinates().release();
                        //delete temp_ptr_to_free;
                        //qrd_geom->getFactory ()->destroyGeometry(qrd_geom);
                        delete qrd_geom;
                    }
                    catch (std::exception &e)
                    {
                    }
                }

                if (temp_pair_geoms)
                {
                    //temp_pair_geoms->first->~Geometry();
                    //temp_pair_geoms->first->getCoordinates().reset();
                    //temp_pair_geoms->first->getCoordinates() = nullptr;
                    //temp_pair_geoms->first->~Geometry();
                    //geos::geom::CoordinateSequence *temp_ptr_to_free = temp_pair_geoms->first->getCoordinates().release();
                    //delete temp_ptr_to_free;
                    //temp_pair_geoms->first->getFactory ()->destroyGeometry(temp_pair_geoms->first);
                    delete temp_pair_geoms->first;
                    delete temp_pair_geoms;
                }
            }

            if (!is_local_list_freed && (queue_counter == num_threads - 1))
            {
                is_local_list_freed = true;

                /* clean all local geometries*/
                /* this section is put here to avoid another worker is still using geometries form this victim*/
                {
                    for (std::list<std::pair<geos::geom::Geometry *, int> *>::iterator itr = list_geoms_1->begin(); itr != list_geoms_1->end(); ++itr)
                    {
                        //(*itr)->first->~Geometry();
                        delete (*itr)->first;
                        //(*itr)->first->getFactory()->destroyGeometry((*itr)->first);
                    }
                    delete list_geoms_1;

                    for (std::list<std::pair<geos::geom::Geometry *, int> *>::iterator itr = list_geoms_2->begin(); itr != list_geoms_2->end(); ++itr)
                    {
                        //(*itr)->first->~Geometry();
                        delete (*itr)->first;
                        //(*itr)->first->getFactory()->destroyGeometry((*itr)->first);
                    }
                    delete list_geoms_2;
                }
            }

            if (is_queue_filled[queue_counter] == -1)
                break;

            /*
            (vect_atomic_counter->at(queue_counter))++;

            spdlog::debug("PRE rank {}, q {} , {} {} ", thread_rank, queue_counter, is_queue_filled[queue_counter], vect_atomic_counter->at(queue_counter) );

            if (vect_atomic_counter->at(queue_counter) >= num_threads)
            {
                vect_atomic_counter->at(queue_counter) = 0;
                is_queue_filled[queue_counter] = 0;
            }*/

            if (thread_rank == 0)
            {
                spdlog::debug("Finished queue {}" ,queue_counter);

                is_queue_filled[queue_counter] = 0;
            }

            ++queue_counter;
        }

        //Final check if any jobs left
        for (int i = 0; i < num_threads; ++i)
        {
            while (!queue[i]->empty())
            {
                std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair_geoms = NULL;
                try
                {
                    std::optional<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> item = queue[i]->steal();
                    temp_pair_geoms = item.value();
                }
                catch (std::bad_optional_access &e)
                {
                    continue;
                }

#ifdef USE_ST_INTERSECTS
                std::unique_ptr<geos::geom::prep::PreparedGeometry> pg =
                    geos::geom::prep::PreparedGeometryFactory::prepare(temp_pair_geoms->first);
#endif //ifdef USE_ST_INTERSECTS

                for (std::vector<geos::geom::Geometry *>::iterator void_itr = temp_pair_geoms->second->begin();
                     void_itr != temp_pair_geoms->second->end(); ++void_itr)
                {
                    geos::geom::Geometry *qrd_geom = *void_itr;
                    try
                    {
#ifdef USE_ST_INTERSECTION
                        std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);
                        local_join_result += geoms_intersection->getNumPoints();
                        //if (geoms_intersection->getNumPoints() != 0)
                        //  ++local_join_result;

                        geoms_intersection = nullptr;

#elif USE_ST_INTERSECTS
                        if (pg->intersects(qrd_geom))
                            ++local_join_result;

#elif USE_ST_UNION
                        std::unique_ptr<geos::geom::Geometry> geoms_union = temp_pair_geoms->first->Union(qrd_geom);

                        if (geoms_union != nullptr)
                        {
                            local_join_result += geoms_union->getNumPoints();
                            geoms_union = nullptr;
                        }

#else //default using USE_ST_INTERSECTION
                        std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);
                        local_join_result += geoms_intersection->getNumPoints();
                        //if (geoms_intersection->getNumPoints() != 0)
                        //    ++local_join_result;

                        geoms_intersection = nullptr;
#endif
                        //qrd_geom->~Geometry();
                        //qrd_geom->getCoordinates().reset();
                        //delete qrd_geom;
                        //qrd_geom->getFactory()->destroyGeometry(qrd_geom);
                    }
                    catch (std::exception &e)
                    {
                    }
                }

                if (temp_pair_geoms)
                {
                    //temp_pair_geoms->first->~Geometry();
                    //temp_pair_geoms->first->getCoordinates().reset();
                    //temp_pair_geoms->first->getFactory ()->destroyGeometry(temp_pair_geoms->first);
                    //delete temp_pair_geoms->first;
                    //delete temp_pair_geoms;
                }
            }
        }

        join_results[thread_rank] = local_join_result;

        auto t_end = std::chrono::steady_clock::now();

        t_diff_temp = t_join_end - t_index_end;
        double t_temp = t_diff_temp.count();
        ;

        t_diff_temp = t_end - t_join_end;
        t_mpi = t_diff_temp.count();

        t_diff_temp = t_end - t_parse_end;
        t_total = t_diff_temp.count();

        spdlog::info("T[{}]: result, {}; total time {}; pop time {}; join time {}: {}; comm time {}",
                     thread_rank, local_join_result, t_total, t_steal, t_join, t_temp, t_mpi);

        //syn_barrier.wait();

        if (thread_rank == 0)
        {
            is_queue_filled[num_threads - 1] = 4;
        }

        spdlog::debug("Leave function {}", "Thread_MPI_spatial_join");
    }

    ulong Thread_Wrapper_MPI_spatial_join_pre_parsing(int num_threads, int num_nb_sendrecv, int num_tasks,
                                                      std::vector<std::list<std::pair<geos::geom::Geometry *, int> *> *> *vect_list_geoms_1,
                                                      std::vector<std::list<std::pair<geos::geom::Geometry *, int> *> *> *vect_list_geoms_2)
    {
        spdlog::debug("Enter function {}", "Thread_Wrapper_MPI_spatial_join_pre_parsing");

        ulong *join_results;
        ulong total_result = 0;

        join_results = (ulong *)malloc(sizeof(ulong) * num_threads);
        //std::thread *threads = new std::thread[num_threads];

        std::vector<std::thread> vect_thread;
        syn_barrier.set_num_threads(num_threads);

        gsj::WSQ_Manager<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> wsq_manager;
        WorkStealingQueue<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> **queue = NULL;
        queue = wsq_manager.WSQ_Init_n_queues(num_threads, pow(2, 20));

        volatile int *is_queue_filled;
        is_queue_filled = (int *)malloc(sizeof(int) * num_threads);

        for (int i = 0; i < num_threads; ++i)
        {
            is_queue_filled[i] = 0;
        }

        is_queue_filled[num_threads - 1] = 3;

        std::thread coord_thread(gsj::Global_Tasks_monitor_thread, queue, num_threads, MPI_COMM_WORLD, is_queue_filled, num_nb_sendrecv, num_tasks);

        for (int i = 0; i < num_threads; ++i)
        {
            core_thread_aff[i] = -1;
            join_results[i] = 0;
            vect_thread.push_back(std::thread(Thread_MPI_spatial_join_pre_parsing, vect_list_geoms_1->at(i), vect_list_geoms_2->at(i),
                                              i, num_threads, queue, is_queue_filled, join_results));
            //threads[i] = std::thread(Thread_MPI_spatial_join_pre_parsing, vect_list_geoms_1->at(i), vect_list_geoms_2->at(i), i, num_threads, queue, is_queue_filled, join_results);
        }

        coord_thread.join();
        for (int i = 0; i < num_threads; ++i)
            vect_thread[i].join();
        //threads[i].join();

        spdlog::debug("Leave function {}", "Thread_Wrapper_MPI_spatial_join_pre_parsing");

        for (int i = 0; i < num_threads; ++i)
            total_result += join_results[i];

        //delete threads;
        free(join_results);
        return total_result;
    }

    void Thread_MPI_unpartition(std::list<geos::geom::Geometry *> *list_geoms_1, std::list<std::pair<geos::geom::Geometry *, geos::geom::Envelope *> *> *list_geoms_2,
                                int thread_rank, int num_threads, WorkStealingQueue<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> **queue,
                                volatile int *is_queue_filled, ulong *join_results)
    {
        ulong local_join_result = 0;
        int numa_node, current_cpu_id;

        spdlog::debug("Enter function {}", "Thread_Spatial_join_multi_owners_parsing");

        double t_total, t_parse, t_steal, t_join, t_mpi;
        std::chrono::duration<double> t_diff_temp;
        t_total = t_parse = t_steal = t_join = t_mpi = 0.0;
        auto t_begin = std::chrono::steady_clock::now();

        current_cpu_id = sched_getcpu();

        numa_node = 0;

#ifdef NUMA_ENABLED
        numa_node = numa_node_of_cpu(current_cpu_id);
#endif //ifdef NUMA_ENABLED

        aff_mutex.lock();
        core_thread_aff[thread_rank] = numa_node;
        aff_mutex.unlock();

        syn_barrier.wait();

        auto t_parse_end = std::chrono::steady_clock::now();
        t_diff_temp = t_parse_end - t_begin;

        if (list_geoms_1 == NULL || list_geoms_2 == NULL)
        {
            spdlog::error("NULL geoms lists");
            return;
        }
        /************************Build index************************************************************************/
        geos::index::strtree::STRtree index_for_layer_1;
        for (std::list<geos::geom::Geometry *>::iterator itr = list_geoms_1->begin(); itr != list_geoms_1->end(); ++itr)
        {
            geos::geom::Geometry *temp_geom = *itr;

            index_for_layer_1.insert(temp_geom->getEnvelopeInternal(), temp_geom);
        }

        auto t_index_end = std::chrono::steady_clock::now();
        t_diff_temp = t_index_end - t_parse_end;

        spdlog::debug("Thread [{0:d}] index built, time {1:03.3f}", thread_rank, t_diff_temp.count());

        /************************Build index end************************************************************************/

        for (std::list<std::pair<geos::geom::Geometry *, geos::geom::Envelope *> *>::iterator itr = list_geoms_2->begin(); itr != list_geoms_2->end(); ++itr)
        {
            std::pair<geos::geom::Geometry *, geos::geom::Envelope *> *temp_geom = (*itr); //->clone().release();
            //geos::geom::Geometry *temp_geom = temp_geom_1->first->clone().release();
            std::vector<void *> results;

            index_for_layer_1.query(temp_geom->second, results);

            if (results.size() != 0)
            {
                std::vector<geos::geom::Geometry *> *temp_geom_vect = new std::vector<geos::geom::Geometry *>();

                for (std::vector<void *>::iterator void_itr = results.begin(); void_itr != results.end(); void_itr++)
                {
                    void *temp_geom_ptr = *void_itr;
                    geos::geom::Geometry *qrd_geom = (geos::geom::Geometry *)temp_geom_ptr;

                    temp_geom_vect->push_back(qrd_geom);
                }

                if (temp_geom_vect->size() <= TASKS_PER_JOB)
                {
                    std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair =
                        new std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *>;

                    temp_pair->first = temp_geom->first;
                    temp_pair->second = temp_geom_vect;

                    queue[thread_rank]->push(temp_pair);
                }
                else
                {
                    uint i, j;
                    for (i = 0; i < temp_geom_vect->size();)
                    {
                        std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair =
                            new std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *>;

                        std::vector<geos::geom::Geometry *> *temp_geom_vect_limited_size = new std::vector<geos::geom::Geometry *>();

                        for (j = 0; j < TASKS_PER_JOB; ++j)
                        {
                            if (i + j < temp_geom_vect->size())
                            {
                                temp_geom_vect_limited_size->push_back(temp_geom_vect->at(i + j));
                            }
                            else
                            {
                                break;
                            }
                        }

                        if (temp_geom_vect_limited_size->empty())
                        {
                            break;
                        }
                        else
                        {
                            temp_pair->first = temp_geom->first;
                            temp_pair->second = temp_geom_vect_limited_size;
                            queue[thread_rank]->push(temp_pair);
                        }
                        i += j;
                    }
                }
            }
        }

        IS_ALL_JOB_PUSHED[thread_rank] = true;

        auto t_push_end = std::chrono::steady_clock::now();
        t_diff_temp = t_push_end - t_index_end;
        spdlog::debug("Thread [{0:d}] all jobs pushed, time {1:03.3f}", thread_rank, t_diff_temp.count());

        bool is_all_job_pushed_temp = true;

        while (1)
        {

            if (thread_rank == 0)
            {
                is_all_job_pushed_temp = true;
                for (int i = 0; i < num_threads; ++i)
                {
                    if (!IS_ALL_JOB_PUSHED[i])
                        is_all_job_pushed_temp = false;
                }

                if (is_all_job_pushed_temp)
                    is_queue_filled[num_threads - 1] = 2;
            }

            auto t_pop_begin = std::chrono::steady_clock::now();

            if (queue[thread_rank]->empty())
                break;

            std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair_geoms = NULL;

            try
            {
                std::optional<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> item = queue[thread_rank]->pop();
                temp_pair_geoms = item.value();
            }
            catch (std::bad_optional_access &e)
            {
                continue;
            }

            auto t_pop_end = std::chrono::steady_clock::now();

#ifdef USE_ST_INTERSECTS
            std::unique_ptr<geos::geom::prep::PreparedGeometry> pg =
                geos::geom::prep::PreparedGeometryFactory::prepare(temp_pair_geoms->first);
#endif //ifdef USE_ST_INTERSECTS

            for (std::vector<geos::geom::Geometry *>::iterator void_itr = temp_pair_geoms->second->begin();
                 void_itr != temp_pair_geoms->second->end(); void_itr++)
            {
                geos::geom::Geometry *qrd_geom = *void_itr;
                try
                {
#ifdef USE_ST_INTERSECTION
                    std::unique_ptr<geos::geom::Geometry> geoms_intersection = qrd_geom->intersection(temp_pair_geoms->first);
                    local_join_result += geoms_intersection->getNumPoints();
                    //if (geoms_intersection->getNumPoints() != 0)
                    //    ++local_join_result;

                    geoms_intersection = nullptr;
#elif USE_ST_INTERSECTS
                    if (pg->intersects(qrd_geom))
                        ++local_join_result;

#elif USE_ST_UNION
                    std::unique_ptr<geos::geom::Geometry> geoms_union = qrd_geom->Union(temp_pair_geoms->first);

                    if (geoms_union != nullptr)
                    {
                        local_join_result += geoms_union->getNumPoints();
                        geoms_union = nullptr;
                    }

#else //default using USE_ST_INTERSECTION
                    std::unique_ptr<geos::geom::Geometry> geoms_intersection = qrd_geom->intersection(temp_pair_geoms->first);
                    local_join_result += geoms_intersection->getNumPoints();
                    //if (geoms_intersection->getNumPoints() != 0)
                    //    ++local_join_result;

                    geoms_intersection = nullptr;
#endif
                }
                catch (std::exception &e)
                {
                }
            }

            if (temp_pair_geoms)
                delete temp_pair_geoms;

            auto t_join_end = std::chrono::steady_clock::now();

            t_diff_temp = t_pop_end - t_pop_begin;
            t_steal += t_diff_temp.count();

            t_diff_temp = t_join_end - t_pop_end;
            t_join += t_diff_temp.count();
        }

        IS_ALL_JOB_DONE[thread_rank] = true;

        //int thread_counter = (thread_rank+1) % num_threads;
        int thread_counter = get_victim_thread_id(num_threads, thread_rank, INIT_VICTIM, true);

        while (thread_counter != NO_MORE_VICTIM)
        {
            auto t_steal_begin = std::chrono::steady_clock::now();

            if (queue[thread_counter]->empty())
            {

                thread_counter = get_victim_thread_id(num_threads, thread_rank, thread_counter, true);
                continue;
            }

            if (!IS_ALL_JOB_PUSHED[thread_counter])
            {
                continue;
            }

            if (thread_rank == 0)
            {
                is_all_job_pushed_temp = true;
                for (int i = 0; i < num_threads; ++i)
                {
                    if (!IS_ALL_JOB_PUSHED[i])
                        is_all_job_pushed_temp = false;
                }

                if (is_all_job_pushed_temp)
                    is_queue_filled[num_threads - 1] = 2;
            }

            std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair_geoms = NULL;
            try
            {
                std::optional<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> item = queue[thread_counter]->steal();
                temp_pair_geoms = item.value();
            }
            catch (std::bad_optional_access &e)
            {
                continue;
            }

            auto t_steal_end = std::chrono::steady_clock::now();

#ifdef USE_ST_INTERSECTS
            std::unique_ptr<geos::geom::prep::PreparedGeometry> pg =
                geos::geom::prep::PreparedGeometryFactory::prepare(temp_pair_geoms->first);
#endif //ifdef USE_ST_INTERSECTS

            for (std::vector<geos::geom::Geometry *>::iterator void_itr = temp_pair_geoms->second->begin();
                 void_itr != temp_pair_geoms->second->end(); ++void_itr)
            {
                geos::geom::Geometry *qrd_geom = *void_itr;
                try
                {
#ifdef USE_ST_INTERSECTION
                    std::unique_ptr<geos::geom::Geometry> geoms_intersection = qrd_geom->intersection(temp_pair_geoms->first);
                    local_join_result += geoms_intersection->getNumPoints();
                    //if (geoms_intersection->getNumPoints() != 0)
                    //    ++local_join_result;

                    geoms_intersection = nullptr;
#elif USE_ST_INTERSECTS
                    if (pg->intersects(qrd_geom))
                        ++local_join_result;

#elif USE_ST_UNION
                    std::unique_ptr<geos::geom::Geometry> geoms_union = qrd_geom->Union(temp_pair_geoms->first);

                    if (geoms_union != nullptr)
                    {
                        local_join_result += geoms_union->getNumPoints();
                        geoms_union = nullptr;
                    }

#else //default using USE_ST_INTERSECTION
                    std::unique_ptr<geos::geom::Geometry> geoms_intersection = qrd_geom->intersection(temp_pair_geoms->first);
                    local_join_result += geoms_intersection->getNumPoints();
                    //if (geoms_intersection->getNumPoints() != 0)
                    //    ++local_join_result;

                    geoms_intersection = nullptr;
#endif
                }
                catch (std::exception &e)
                {
                }
            }

            if (temp_pair_geoms)
                delete temp_pair_geoms;

            auto t_join_end = std::chrono::steady_clock::now();

            t_diff_temp = t_steal_end - t_steal_begin;
            t_steal += t_diff_temp.count();

            t_diff_temp = t_join_end - t_steal_end;
            t_join += t_diff_temp.count();
        }

        auto t_join_end = std::chrono::steady_clock::now();

        if (thread_rank == 0 && !is_all_job_pushed_temp)
        {
            is_all_job_pushed_temp = true;
            is_queue_filled[num_threads - 1] = 2;
        }

        if (thread_rank == 0)
        {
            if (is_queue_filled[num_threads - 1] == 3)
                is_queue_filled[num_threads - 1] = 2;
            spdlog::debug("All local jobs are done");
        }

        bool is_local_list_freed = false;

        // Now waiting for jobs from other
        int queue_counter = 0;

        while (1)
        {
            queue_counter = queue_counter % num_threads;

            //spdlog::info("{} {}",queue_counter, is_queue_filled[queue_counter]);

            if (is_queue_filled[queue_counter] == 0)
            {
                //queue_counter = (queue_counter+1)% num_threads;
                std::this_thread::sleep_for(std::chrono::nanoseconds(50));
            }

            //spdlog::info("{} {}",queue_counter, is_queue_filled[queue_counter]);

            while (!queue[queue_counter]->empty())
            {
                std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair_geoms = NULL;
                try
                {
                    std::optional<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> item = queue[queue_counter]->steal();
                    temp_pair_geoms = item.value();
                }
                catch (std::bad_optional_access &e)
                {
                    continue;
                }

#ifdef USE_ST_INTERSECTS
                std::unique_ptr<geos::geom::prep::PreparedGeometry> pg =
                    geos::geom::prep::PreparedGeometryFactory::prepare(temp_pair_geoms->first);
#endif //ifdef USE_ST_INTERSECTS

                for (std::vector<geos::geom::Geometry *>::iterator void_itr = temp_pair_geoms->second->begin();
                     void_itr != temp_pair_geoms->second->end(); ++void_itr)
                {
                    geos::geom::Geometry *qrd_geom = *void_itr;
                    try
                    {
#ifdef USE_ST_INTERSECTION
                        std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);
                        local_join_result += geoms_intersection->getNumPoints();
                        //if (geoms_intersection->getNumPoints() != 0)
                        //    ++local_join_result;

                        geoms_intersection = nullptr;

#elif USE_ST_INTERSECTS
                        if (pg->intersects(qrd_geom))
                            ++local_join_result;

#elif USE_ST_UNION
                        std::unique_ptr<geos::geom::Geometry> geoms_union = temp_pair_geoms->first->Union(qrd_geom);

                        if (geoms_union != nullptr)
                        {
                            local_join_result += geoms_union->getNumPoints();
                            geoms_union = nullptr;
                        }

#else //default using USE_ST_INTERSECTION
                        std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);

                        local_join_result += geoms_intersection->getNumPoints();
                        //if (geoms_intersection->getNumPoints() != 0)
                        //    ++local_join_result;

                        geoms_intersection = nullptr;
#endif
                        //qrd_geom->getCoordinates() = nullptr;
                        //qrd_geom->getCoordinates().reset();
                        //delete qrd_geom;
                        qrd_geom->~Geometry();
                        //geos::geom::CoordinateSequence *temp_ptr_to_free = qrd_geom->getCoordinates().release();
                        //delete temp_ptr_to_free;
                        //qrd_geom->getFactory ()->destroyGeometry(qrd_geom);
                        //delete qrd_geom;
                    }
                    catch (std::exception &e)
                    {
                    }
                }

                if (temp_pair_geoms)
                {
                    temp_pair_geoms->first->~Geometry();
                    //temp_pair_geoms->first->getCoordinates().reset();
                    //temp_pair_geoms->first->getCoordinates() = nullptr;
                    //temp_pair_geoms->first->~Geometry();
                    //geos::geom::CoordinateSequence *temp_ptr_to_free = temp_pair_geoms->first->getCoordinates().release();
                    //delete temp_ptr_to_free;
                    //temp_pair_geoms->first->getFactory ()->destroyGeometry(temp_pair_geoms->first);
                    //delete temp_pair_geoms->first;
                    delete temp_pair_geoms;
                }
            }

            if (!is_local_list_freed && (queue_counter == num_threads - 1))
            {
                is_local_list_freed = true;

                /* clean all local geometries*/
                /* this section is put here to avoid another worker is still using geometries form this victim*/
                {
                    for (std::list<geos::geom::Geometry *>::iterator itr = list_geoms_1->begin(); itr != list_geoms_1->end(); ++itr)
                    {
                        (*itr)->~Geometry();
                        //delete (*itr);
                        //(*itr)->first->getFactory()->destroyGeometry((*itr)->first);
                    }
                    delete list_geoms_1;

                    if (thread_rank == 0)
                    {
                        for (std::list<std::pair<geos::geom::Geometry *, geos::geom::Envelope *> *>::iterator itr = list_geoms_2->begin(); itr != list_geoms_2->end(); ++itr)
                        {
                            (*itr)->first->~Geometry();
                            //delete (*itr)->first;
                            delete (*itr)->second;
                            //(*itr)->first->getFactory()->destroyGeometry((*itr)->first);
                        }
                        delete list_geoms_2;
                    }
                }
            }

            if (is_queue_filled[queue_counter] == -1)
                break;

            if (thread_rank == 0)
            {
                is_queue_filled[queue_counter] = 0;
            }

            ++queue_counter;
        }

        //Final check if any jobs left
        for (int i = 0; i < num_threads; ++i)
        {
            while (!queue[i]->empty())
            {
                std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair_geoms = NULL;
                try
                {
                    std::optional<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> item = queue[i]->steal();
                    temp_pair_geoms = item.value();
                }
                catch (std::bad_optional_access &e)
                {
                    continue;
                }

#ifdef USE_ST_INTERSECTS
                std::unique_ptr<geos::geom::prep::PreparedGeometry> pg =
                    geos::geom::prep::PreparedGeometryFactory::prepare(temp_pair_geoms->first);
#endif //ifdef USE_ST_INTERSECTS

                for (std::vector<geos::geom::Geometry *>::iterator void_itr = temp_pair_geoms->second->begin();
                     void_itr != temp_pair_geoms->second->end(); ++void_itr)
                {
                    geos::geom::Geometry *qrd_geom = *void_itr;
                    try
                    {
#ifdef USE_ST_INTERSECTION
                        std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);
                        local_join_result += geoms_intersection->getNumPoints();
                        //if (geoms_intersection->getNumPoints() != 0)
                        //  ++local_join_result;

                        geoms_intersection = nullptr;

#elif USE_ST_INTERSECTS
                        if (pg->intersects(qrd_geom))
                            ++local_join_result;

#elif USE_ST_UNION
                        std::unique_ptr<geos::geom::Geometry> geoms_union = temp_pair_geoms->first->Union(qrd_geom);

                        if (geoms_union != nullptr)
                        {
                            local_join_result += geoms_union->getNumPoints();
                            geoms_union = nullptr;
                        }

#else //default using USE_ST_INTERSECTION
                        std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_pair_geoms->first->intersection(qrd_geom);
                        local_join_result += geoms_intersection->getNumPoints();
                        //if (geoms_intersection->getNumPoints() != 0)
                        //    ++local_join_result;

                        geoms_intersection = nullptr;
#endif
                        //qrd_geom->~Geometry();
                        //qrd_geom->getCoordinates().reset();
                        //delete qrd_geom;
                        //qrd_geom->getFactory()->destroyGeometry(qrd_geom);
                    }
                    catch (std::exception &e)
                    {
                    }
                }

                if (temp_pair_geoms)
                {
                    //temp_pair_geoms->first->~Geometry();
                    //temp_pair_geoms->first->getCoordinates().reset();
                    //temp_pair_geoms->first->getFactory ()->destroyGeometry(temp_pair_geoms->first);
                    //delete temp_pair_geoms->first;
                    //delete temp_pair_geoms;
                }
            }
        }

        join_results[thread_rank] = local_join_result;

        auto t_end = std::chrono::steady_clock::now();

        t_diff_temp = t_join_end - t_index_end;
        double t_temp = t_diff_temp.count();

        t_diff_temp = t_end - t_join_end;
        t_mpi = t_diff_temp.count();

        t_diff_temp = t_end - t_parse_end;
        t_total = t_diff_temp.count();

        spdlog::info("T[{}]: result, {}; total time {}; pop time {}; join time {}: {}; comm time {}",
                     thread_rank, local_join_result, t_total, t_steal, t_join, t_temp, t_mpi);

        syn_barrier.wait();

        if (thread_rank == 0)
        {
            is_queue_filled[num_threads - 1] = 4;
        }

        spdlog::debug("Leave function {}", "Thread_MPI_spatial_join");
    }

    ulong Thread_Wrapper_MPI_unpartition(int num_threads, int num_nb_sendrecv, int num_tasks,
                                         std::vector<std::list<geos::geom::Geometry *> *> *vect_list_geoms_1,
                                         std::list<std::pair<geos::geom::Geometry *, geos::geom::Envelope *> *> *list_geoms_2)
    {
        spdlog::debug("Enter function {}", "Thread_Wrapper_MPI_spatial_join_pre_parsing");

        ulong *join_results;
        ulong total_result = 0;

        join_results = (ulong *)malloc(sizeof(ulong) * num_threads);
        std::vector<std::thread> vect_thread;
        syn_barrier.set_num_threads(num_threads);

        gsj::WSQ_Manager<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> wsq_manager;
        WorkStealingQueue<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> **queue = NULL;
        queue = wsq_manager.WSQ_Init_n_queues(num_threads, pow(2, 14));

        volatile int *is_queue_filled;
        is_queue_filled = (int *)malloc(sizeof(int) * num_threads);

        for (int i = 0; i < num_threads; ++i)
        {
            is_queue_filled[i] = 0;
        }

        is_queue_filled[num_threads - 1] = 3;

        std::thread coord_thread(gsj::Global_Tasks_monitor_thread, queue, num_threads, MPI_COMM_WORLD, is_queue_filled, num_nb_sendrecv, num_tasks);

        for (int i = 0; i < num_threads; ++i)
        {
            core_thread_aff[i] = -1;
            join_results[i] = 0;
            vect_thread.push_back(std::thread(Thread_MPI_unpartition, vect_list_geoms_1->at(i), list_geoms_2,
                                              i, num_threads, queue, is_queue_filled, join_results));
        }

        coord_thread.join();
        for (int i = 0; i < num_threads; ++i)
            vect_thread[i].join();
        //threads[i].join();

        spdlog::debug("Leave function {}", "Thread_Wrapper_MPI_spatial_join_pre_parsing");

        for (int i = 0; i < num_threads; ++i)
            total_result += join_results[i];

        //delete threads;
        free(join_results);
        return total_result;
    }

} //namespace gsj

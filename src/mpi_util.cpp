#include <mpi_util.h>

namespace gsj
{
    namespace
    { //Nested anonymous namespace. Functions are only visible in this cpp.

        /*************** Function List ***************
     * void recv_buf_parse()
     * void send_buf_gen()
     * void send_tasks()
     * void recv_tasks()
    *************** End function List ************/

        gsj::thread_barrier parse_thread_barrier(2);
        std::mutex parse_list_mutex;
        /* This function is used for send [num_tasks] join tasks to [target] using [tag] as MPI tag. */
        /* It can be called as a thread function */
        void send_tasks(int target, int tag, int num_tasks, int num_threads, MPI_Comm comm,
                        WorkStealingQueue<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> **queue,
                        std::mutex *steal_mutex)
        {
            //spdlog::debug("SEND {} {}", target, tag);
            MPI_Request requests[4];

            std::list<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> *list_geoms_to_send =
                new std::list<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *>();

            //steal_mutex->lock();
            for (int i = 0; i < num_threads; ++i)
            {
                while (!queue[i]->empty() && list_geoms_to_send->size() < (uint)num_tasks)
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

                    if (temp_pair_geoms != NULL)
                    {
                        list_geoms_to_send->push_back(temp_pair_geoms);
                    }
                }
            }
            //steal_mutex->unlock();

            ulong send_buf[4];

            if (!list_geoms_to_send->empty())
            {
                /* Send data to target*/
                int geoms_buf_size;

                double *send_buf_geoms = NULL;
                ulong *send_buf_sizes = NULL;
                ulong *send_map_arr = NULL;
                MPI_Util_send_buf_gen(list_geoms_to_send, send_buf, send_buf_geoms, send_buf_sizes, send_map_arr);
                //spdlog::debug("SENDed {} {} {} {} {} {}", target, tag, send_buf[0], send_buf[1], send_buf[2], send_buf[3]);
                assert(send_buf_geoms != NULL);
                assert(send_buf_sizes != NULL);
                assert(send_map_arr != NULL);

                MPI_Isend(send_buf, 4, MPI_UNSIGNED_LONG, target, tag, comm, &requests[0]);
                MPI_Isend(send_buf_sizes, 2 * (send_buf[0] + send_buf[2]), MPI_UNSIGNED_LONG, target, tag + 1, comm, &requests[1]);
                MPI_Isend(send_map_arr, send_buf[0], MPI_UNSIGNED_LONG, target, tag + 2, comm, &requests[2]);

                geoms_buf_size = (send_buf[1] + send_buf[3]) * 2;

                MPI_Isend(send_buf_geoms, geoms_buf_size, MPI_DOUBLE, target, tag + 3, comm, &requests[3]);

                bool is_all_isend_finished = false;

                while (!is_all_isend_finished)
                {
                    is_all_isend_finished = true;

                    int is_isend_finished = 0;

                    MPI_Test(&requests[0], &is_isend_finished, MPI_STATUS_IGNORE);

                    if (!is_isend_finished)
                        is_all_isend_finished = false;

                    is_isend_finished = 0;
                    MPI_Test(&requests[1], &is_isend_finished, MPI_STATUS_IGNORE);

                    if (!is_isend_finished)
                        is_all_isend_finished = false;

                    is_isend_finished = 0;
                    MPI_Test(&requests[2], &is_isend_finished, MPI_STATUS_IGNORE);

                    if (!is_isend_finished)
                        is_all_isend_finished = false;

                    is_isend_finished = 0;
                    MPI_Test(&requests[3], &is_isend_finished, MPI_STATUS_IGNORE);

                    if (!is_isend_finished)
                        is_all_isend_finished = false;

                    if (!is_all_isend_finished)
                        std::this_thread::sleep_for(std::chrono::nanoseconds(100));
                }

                free(send_buf_sizes);
                free(send_map_arr);
                free(send_buf_geoms);
            }
            else
            {
                /* Send stop signal to all processes*/
                send_buf[0] = send_buf[2] = send_buf[3] = 0;
                send_buf[1] = STOP_SIGN_NB_SENDRECV;

                MPI_Isend(send_buf, 4, MPI_UNSIGNED_LONG, target, tag, comm, &requests[0]);
                spdlog::debug("THREAD End sending jobs to {}", target);
            }

            //spdlog::debug("END SEND {} {}", target, tag);
            //spdlog::info("Leave {} sendbuf {} {} {} {} {}", "send_tasks", send_buf[0], send_buf[1], send_buf[2], send_buf[3], send_buf[4]);
            delete list_geoms_to_send;
        }

        /* This function is used for recv join tasks to [source] using [tag] as MPI tag. Received tasks will be pushed into [queue]*/
        /* It should be called as a thread function */
        void recv_tasks(int source, int tag, MPI_Comm comm, volatile int *is_all_job_finished, std::list<std::vector<void *> *> *recv_buf_list)
        {
            //spdlog::debug("RECV {} {}", source, tag);
            //MPI_Status status;
            MPI_Request requests[4];
            int is_buf_received = 0;

            ulong *recv_buf;
            recv_buf = (ulong *)malloc(sizeof(ulong) * 4);

            MPI_Irecv(recv_buf, 4, MPI_UNSIGNED_LONG, source, tag, comm, &requests[0]);

            while (!is_buf_received)
            {
                MPI_Test(&requests[0], &is_buf_received, MPI_STATUS_IGNORE);

                /* another thread received stop signal*/
                if (!is_buf_received && (*is_all_job_finished) && (*is_all_job_finished) < tag)
                {
                    spdlog::debug("THREAD End sending received by other thread {} {}", *is_all_job_finished, tag);

                    if (requests[0] != MPI_REQUEST_NULL)
                        MPI_Cancel(&requests[0]);

                    free(recv_buf);
                    return;
                }
                else
                    std::this_thread::sleep_for(std::chrono::nanoseconds(100));
            }

            //spdlog::debug("RECVed {} {} {} {} {} {}", source, tag, recv_buf[0], recv_buf[1], recv_buf[2], recv_buf[3]);
            if (recv_buf[0] == 0 && recv_buf[1] == STOP_SIGN_NB_SENDRECV)
            {
                //spdlog::debug("End sign received from {}", source);
                if (recv_buf[3] == TEMP_STOP_SIGN_NB_SENDRECV)
                    *is_all_job_finished = tag + 1;
                else if (tag)
                    *is_all_job_finished = tag;
                else /* in case tag is 0*/
                    *is_all_job_finished = 2;

                free(recv_buf);

                return;
            }

            std::vector<void *> *temp_recvbuf_vect = new std::vector<void *>(4);

            int recv_geoms_buf_size = (recv_buf[1] + recv_buf[3]) * 2;
            int recv_sizes_size = (recv_buf[0] + recv_buf[2]) * 2;

            double *recv_buf_geoms = (double *)malloc(sizeof(double) * recv_geoms_buf_size);
            assert(recv_buf_geoms);
            ulong *recv_buf_sizes = (ulong *)malloc(sizeof(ulong) * recv_sizes_size);
            assert(recv_buf_sizes);
            ulong *recv_map_arr = (ulong *)malloc(sizeof(ulong) * recv_buf[0]);
            assert(recv_map_arr);

            MPI_Irecv(recv_buf_sizes, recv_sizes_size, MPI_UNSIGNED_LONG, source, tag + 1, comm, &requests[1]);
            MPI_Irecv(recv_map_arr, recv_buf[0], MPI_UNSIGNED_LONG, source, tag + 2, comm, &requests[2]);
            MPI_Irecv(recv_buf_geoms, recv_geoms_buf_size, MPI_DOUBLE, source, tag + 3, comm, &requests[3]);

            bool is_all_irecv_finished = false;

            while (!is_all_irecv_finished)
            {
                is_all_irecv_finished = true;

                int is_irecv_finished = 0;

                MPI_Test(&requests[1], &is_irecv_finished, MPI_STATUS_IGNORE);

                if (!is_irecv_finished)
                    is_all_irecv_finished = false;

                is_irecv_finished = 0;
                MPI_Test(&requests[2], &is_irecv_finished, MPI_STATUS_IGNORE);

                if (!is_irecv_finished)
                    is_all_irecv_finished = false;

                is_irecv_finished = 0;
                MPI_Test(&requests[3], &is_irecv_finished, MPI_STATUS_IGNORE);

                if (!is_irecv_finished)
                    is_all_irecv_finished = false;

                if (!is_all_irecv_finished)
                    std::this_thread::sleep_for(std::chrono::nanoseconds(100));
            }

            temp_recvbuf_vect->at(0) = (void *)recv_buf;
            temp_recvbuf_vect->at(1) = (void *)recv_buf_geoms;
            temp_recvbuf_vect->at(2) = (void *)recv_buf_sizes;
            temp_recvbuf_vect->at(3) = (void *)recv_map_arr;

            parse_list_mutex.lock();
            recv_buf_list->push_back(temp_recvbuf_vect);
            parse_list_mutex.unlock();
            //spdlog::debug("END RECV {} {}", source, tag);
        }

        void combine_dupilcate_geoms(std::list<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> *list_geoms)
        {
            assert(list_geoms);
            //spdlog::debug("combine_dupilcate_geoms begin");
            std::list<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> *temp_list_geoms = new std::list<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *>();
            std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair_geoms = NULL;

            for (std::list<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *>::iterator itr = list_geoms->begin();
                 itr != list_geoms->end(); ++itr)
            {
                if (temp_pair_geoms == NULL)
                {
                    temp_pair_geoms = *itr;
                }
                else if ((*itr)->first != temp_pair_geoms->first)
                {
                    temp_list_geoms->push_back(temp_pair_geoms);
                    temp_pair_geoms = *itr;
                }
                else
                {
                    while (!((*itr)->second->empty()))
                    {
                        temp_pair_geoms->second->push_back((*itr)->second->back());
                        (*itr)->second->pop_back();
                    }
                }
            }

            //spdlog::debug("temp_list_geoms size {}", temp_list_geoms->size());

            if (temp_pair_geoms != NULL)
                temp_list_geoms->push_back(temp_pair_geoms);

            list_geoms->clear();
            list_geoms->resize(0);

            while (!(temp_list_geoms->empty()))
            {
                list_geoms->push_back(temp_list_geoms->back());
                temp_list_geoms->pop_back();
            }
            //spdlog::debug("combine_dupilcate_geoms end");
        }

        void parse_thread_wrapper(uint num_parse_thread, int num_worker_threads,
                                  WorkStealingQueue<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> **queue,
                                  std::list<std::vector<void *> *> *recv_buf_list, volatile int *parse_list_status, volatile int *is_queue_filled)
        {
            spdlog::debug("Enter {}, num_parse_thread {}, worker {}, ", "parse_thread_wrapper", num_parse_thread, num_worker_threads);

            uint local_queue_counter = 0;

            std::mutex parse_push_mutex;
            std::vector<std::thread> vect_threads;

            while (*parse_list_status == 0)
            {
                while (recv_buf_list->empty() && *parse_list_status == 0)
                {
                    std::this_thread::sleep_for(std::chrono::nanoseconds(50));
                }

                local_queue_counter = local_queue_counter%num_worker_threads;
                //spdlog::debug("FILING q{}", local_queue_counter);

                vect_threads.clear();
                parse_list_mutex.lock();
                while (!recv_buf_list->empty() && vect_threads.size() < num_parse_thread)
                {
                    std::vector<void *> *temp_void_vect = recv_buf_list->back();

                    assert(temp_void_vect->size() == 4);

                    ulong *recv_buf = (ulong *)(temp_void_vect->at(0));
                    double *recv_buf_geoms = (double *)(temp_void_vect->at(1));
                    ulong *recv_buf_sizes = (ulong *)(temp_void_vect->at(2));
                    ulong *recv_map_arr = (ulong *)(temp_void_vect->at(3));

                    std::thread temp_thread(MPI_Util_parse_recv_buf, queue[local_queue_counter], recv_buf, recv_buf_geoms, recv_buf_sizes, recv_map_arr, &parse_push_mutex);
                    vect_threads.push_back(std::move(temp_thread));
                    recv_buf_list->pop_back();
                }
                parse_list_mutex.unlock();

                for (uint i = 0; i < vect_threads.size(); ++i)
                {
                    vect_threads[i].join();
                }

                //spdlog::debug("END FILING q{}", local_queue_counter);

                is_queue_filled[local_queue_counter] = 1;

                if (is_queue_filled[(local_queue_counter + 1) % num_worker_threads] != 1)
                    local_queue_counter = (local_queue_counter + 1) % num_worker_threads;
            }

            spdlog::debug("Parse wrapper perform last check");

            while (!recv_buf_list->empty())
            {
                vect_threads.clear();
                while (!recv_buf_list->empty() && vect_threads.size() < num_parse_thread)
                {
                    std::vector<void *> *temp_void_vect = recv_buf_list->back();

                    assert(temp_void_vect->size() == 4);

                    ulong *recv_buf = (ulong *)(temp_void_vect->at(0));
                    double *recv_buf_geoms = (double *)(temp_void_vect->at(1));
                    ulong *recv_buf_sizes = (ulong *)(temp_void_vect->at(2));
                    ulong *recv_map_arr = (ulong *)(temp_void_vect->at(3));

                    std::thread temp_thread(MPI_Util_parse_recv_buf, queue[local_queue_counter], recv_buf, recv_buf_geoms, recv_buf_sizes, recv_map_arr, &parse_push_mutex);
                    vect_threads.push_back(std::move(temp_thread));
                    recv_buf_list->pop_back();
                }

                for (uint i = 0; i < vect_threads.size(); ++i)
                {
                    vect_threads[i].join();
                }

                is_queue_filled[local_queue_counter] = 1;

                if (is_queue_filled[(local_queue_counter + 1) % num_worker_threads] != 1)
                    local_queue_counter = (local_queue_counter + 1) % num_worker_threads;
            }

            for (int i = 0; i < num_worker_threads; ++i)
                is_queue_filled[i] = -1;

            *parse_list_status = 0;
            spdlog::debug("Leave {}", "parse_thread_wrapper");
        }

    } //End nested anonymous namespace.

    void Global_Tasks_monitor_thread(WorkStealingQueue<std::pair<geos::geom::Geometry *,
                                                                 std::vector<geos::geom::Geometry *> *> *> **queue,
                                     int num_threads, MPI_Comm comm, volatile int *is_queue_filled, int num_nb_sendrecv, int num_tasks)
    {
        int my_world_rank, num_world_nodes;
        MPI_Comm_size(comm, &num_world_nodes);
        MPI_Comm_rank(comm, &my_world_rank);

        spdlog::set_pattern("P" + std::to_string(my_world_rank) + " [%H:%M:%S.%e] %v");
#ifdef DEBUG
        spdlog::set_level(spdlog::level::debug); // Set log level to debug
#else
        spdlog::set_level(spdlog::level::info); // Set log level to info
#endif
        spdlog::debug("Global_Tasks_monitor_thread begin");

        /* To control memory usage, RAM*/
        struct sysinfo mem_info;
        sysinfo(&mem_info);

        long long total_phys_mem = mem_info.totalram;
        //Multiply in next statement to avoid int overflow on right hand side...
        total_phys_mem *= mem_info.mem_unit;

        /* Setup RMA window*/
        long unsigned int *my_base = NULL;
        long unsigned int *put_buf = NULL;
        long unsigned int *get_buf = NULL;
        MPI_Win global_win;

        MPI_Alloc_mem(sizeof(long unsigned int) * 2, MPI_INFO_NULL, &my_base);
        MPI_Alloc_mem(sizeof(long unsigned int) * 2, MPI_INFO_NULL, &put_buf);
        MPI_Alloc_mem(sizeof(long unsigned int) * 2, MPI_INFO_NULL, &get_buf);

        get_buf[0] = get_buf[1] = 0;
        put_buf[0] = put_buf[1] = 0;
        my_base[0] = my_base[1] = RMA_NOT_READY;

        MPI_Win_create(my_base, sizeof(long unsigned int) * 2, sizeof(long unsigned int), MPI_INFO_NULL, comm,
                       &global_win);

        MPI_Win_fence(0, global_win);

        /* init starving status and send tags */
        int *starve_proc;
        int *send_tags;
        int *recv_tags;
        int *temp_stop_counter;

        starve_proc = (int *)malloc(sizeof(int) * num_world_nodes);
        send_tags = (int *)malloc(sizeof(int) * num_world_nodes);
        recv_tags = (int *)malloc(sizeof(int) * num_world_nodes);
        temp_stop_counter = (int *)malloc(sizeof(int) * num_world_nodes);

        for (int i = 0; i < num_world_nodes; ++i)
        {
            starve_proc[i] = 0;
            send_tags[i] = 0;
            recv_tags[i] = 0;
            temp_stop_counter[i] = 0;
        }

        /* Get upper bound of MPI_TAG of comm */
        void *tag_ub_void;
        int tag_ub_flag;
        int tag_ub_val;

        MPI_Comm_get_attr(comm, MPI_TAG_UB, &tag_ub_void, &tag_ub_flag);
        if (!tag_ub_flag)
        {
            spdlog::error("Could not get TAG_UB");
            exit(1);
        }
        else
        {
            tag_ub_val = *(int *)tag_ub_void;
        }

        /* Save some room for safety */
        tag_ub_val = tag_ub_val / 5 * 4;

        /* Check and provide tasks for other nodes */
        //Wait for all tasks being pushed

        while (is_queue_filled[num_threads - 1] == 3)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        is_queue_filled[num_threads - 1] = 0;

        MPI_Win_lock(MPI_LOCK_EXCLUSIVE, my_world_rank, 0, global_win);
        put_buf[0] = WAIT_FOR_TASKS_REQUEST;
        put_buf[1] = 0;
        MPI_Put(put_buf, 2, MPI_UNSIGNED_LONG, my_world_rank, 0, 2, MPI_UNSIGNED_LONG, global_win);

        MPI_Win_flush(my_world_rank, global_win);
        MPI_Win_unlock(my_world_rank, global_win);

        /* Check current free memory */
        sysinfo(&mem_info);
        long long current_free_mem = mem_info.freeram;
        current_free_mem *= mem_info.mem_unit;

        spdlog::debug("Wait for task request begin {}, Total RAM {} : {}", is_queue_filled[0], current_free_mem, total_phys_mem);
        int num_targets = 0;
        int send_itr_counter = 0;
        std::mutex isend_steal_mutex;

        int stop_send_tasks_threshold = num_tasks * num_nb_sendrecv * num_world_nodes * 2;

        //TEMP usage, rm later
        int temp_divider = (num_world_nodes < 5) ? 5 : (num_world_nodes + 1);

#ifdef USE_ST_UNION
        temp_divider *= 8;
#else
        temp_divider *= 2;
        //stop_send_tasks_threshold *= 2;
#endif

        /* Begin in sending status */
        while (1)
        {
            int num_local_tasks = 0;

            for (int i = 0; i < num_threads; ++i)
            {
                num_local_tasks += queue[i]->size();
            }

            if (num_local_tasks < stop_send_tasks_threshold)
            {
                break;
            }

            if (send_itr_counter == 0)
            {
                MPI_Win_lock(MPI_LOCK_EXCLUSIVE, my_world_rank, 0, global_win);
                MPI_Win_flush(my_world_rank, global_win);
                MPI_Get(get_buf, 2, MPI_UNSIGNED_LONG, my_world_rank,
                        0, 2, MPI_UNSIGNED_LONG, global_win);
                MPI_Win_flush(my_world_rank, global_win);

                if (get_buf[0] == REQUEST_FOR_TASKS)
                {
                    int target = get_buf[1];

                    /* Exit if unexpected target received, indicate errors in code */
                    assert(target >= 0 && target < num_world_nodes);

                    assert(starve_proc[target] == 0);

                    starve_proc[target] = 1;
                    ++num_targets;

                    if (num_targets * num_nb_sendrecv >= ISEND_TARGETS_CAP)
                    {
                        put_buf[0] = ALL_LOCAL_TASKS_COMPLETED;
                        put_buf[1] = ALL_LOCAL_TASKS_COMPLETED;
                    }
                    else
                    {
                        put_buf[0] = WAIT_FOR_TASKS_REQUEST;
                        put_buf[1] = num_local_tasks;
                    }
                    MPI_Put(put_buf, 2, MPI_UNSIGNED_LONG, my_world_rank,
                            0, 2, MPI_UNSIGNED_LONG, global_win);

                    MPI_Win_flush(my_world_rank, global_win);

                    temp_stop_counter[target] = 0;

                    spdlog::debug("Send jobs to {} begin with tag {}, counter {}, #targets {}", target, send_tags[target], send_itr_counter, num_targets);
                }
                else if (num_targets == 0)
                {
                    put_buf[0] = WAIT_FOR_TASKS_REQUEST;
                    put_buf[1] = num_local_tasks;
                    MPI_Put(put_buf, 2, MPI_UNSIGNED_LONG, my_world_rank, 0, 2, MPI_UNSIGNED_LONG, global_win);
                    MPI_Win_flush(my_world_rank, global_win);
                }

                MPI_Win_unlock(my_world_rank, global_win);
            }

            if (num_targets > 0)
            {
                std::vector<std::thread> vect_sendtasks;
                vect_sendtasks.clear();

                for (int target = 0; target < num_world_nodes; ++target)
                {
                    if (starve_proc[target] == 1)
                    {
                        std::thread temp_thread(send_tasks, target, send_tags[target], num_tasks, num_threads, comm, queue, &isend_steal_mutex);

                        vect_sendtasks.push_back(std::move(temp_thread));

                        send_tags[target] += 10;
                    }

                    if (send_tags[target] > tag_ub_val)
                        send_tags[target] = 0;
                }

                ++send_itr_counter;

                for (uint i = 0; i < vect_sendtasks.size(); ++i)
                {
                    /* for thread 0 to num_nb_sendrecv-2, detach; for thread num_nb_sendrecv-1, join */
                    /* Here performs a parallel isend  but in different loop iterations */
                    if (send_itr_counter == num_nb_sendrecv)
                    {
                        vect_sendtasks.at(i).join();
                    }
                    else
                    {
                        vect_sendtasks.at(i).detach();
                    }
                }

                /* Flow control, get ACKs from starving processes*/
                if (send_itr_counter == num_nb_sendrecv)
                {
                    send_itr_counter = 0;
                    MPI_Request requests[num_world_nodes];
                    int num_tasks_in_node[num_world_nodes];

                    for (int target = 0; target < num_world_nodes; ++target)
                    {
                        num_tasks_in_node[target] = 0;
                        if (starve_proc[target] == 1)
                        {
                            MPI_Irecv(&num_tasks_in_node[target], 1, MPI_INT, target, ACK_TAG, comm, &requests[target]);
                            //spdlog::debug("Wait ACK from {}", target);
                        }
                    }

                    num_local_tasks = 0;

                    for (int i = 0; i < num_threads; ++i)
                    {
                        num_local_tasks += queue[i]->size();
                    }

                    int num_task_flow_control = num_local_tasks / temp_divider; /// (num_world_nodes + 1);
                    for (int target = 0; target < num_world_nodes; ++target)
                    {
                        if (starve_proc[target] == 1)
                        {
                            int is_node_responsed = 0;
                            while (!is_node_responsed)
                            {
                                MPI_Test(&requests[target], &is_node_responsed, MPI_STATUS_IGNORE);

                                if (!is_node_responsed)
                                {
                                    std::this_thread::sleep_for(std::chrono::nanoseconds(50));
                                }
                            }

                            //spdlog::debug("Received ACK from {}, tasks {}", target, num_tasks_in_node[target]);

                            //50000 threshold is a temporary method
                            //if (num_tasks_in_node[target] > num_task_flow_control || num_tasks_in_node[target] > 50000)
                            if (num_tasks_in_node[target] > NUM_TASK_FLOW_CONTROL || num_tasks_in_node[target] > num_task_flow_control)
                            {
                                temp_stop_counter[target]++;

                                if (temp_stop_counter[target] > 1)
                                {
                                    ulong send_buf[4];
                                    send_buf[0] = send_buf[2] = 0;
                                    send_buf[1] = STOP_SIGN_NB_SENDRECV;
                                    if (num_local_tasks > stop_send_tasks_threshold)
                                    {
                                        /* send temp stop sign. the other nodes can ask for jobs later */
                                        send_buf[3] = TEMP_STOP_SIGN_NB_SENDRECV;
                                        MPI_Isend(send_buf, 4, MPI_UNSIGNED_LONG, target, send_tags[target], comm, &requests[target]);
                                        spdlog::debug("Temporary end sending jobs to {} with tasks {}, sender has {} tasks left", target, num_tasks_in_node[target], num_local_tasks);

                                        if (num_targets * num_nb_sendrecv >= ISEND_TARGETS_CAP)
                                        {
                                            /* Open to other nodes again */
                                            MPI_Win_lock(MPI_LOCK_EXCLUSIVE, my_world_rank, 0, global_win);
                                            put_buf[0] = WAIT_FOR_TASKS_REQUEST;
                                            put_buf[1] = num_local_tasks;
                                            MPI_Put(put_buf, 2, MPI_UNSIGNED_LONG, my_world_rank, 0, 2, MPI_UNSIGNED_LONG, global_win);

                                            MPI_Win_flush(my_world_rank, global_win);
                                            MPI_Win_unlock(my_world_rank, global_win);
                                        }
                                    }
                                    else
                                    {
                                        send_buf[3] = 0;
                                        MPI_Isend(send_buf, 4, MPI_UNSIGNED_LONG, target, send_tags[target], comm, &requests[target]);
                                        spdlog::debug("End sending jobs to {}", target);
                                    }

                                    /* Peform a dummy recv to take extra isend from target*/
                                    int dummy_recv;
                                    MPI_Irecv(&dummy_recv, 1, MPI_INT, target, ACK_TAG, comm, &requests[target]);

                                    temp_stop_counter[target] = 0;
                                    send_tags[target] += (10 * num_nb_sendrecv);
                                    starve_proc[target] = 0;
                                    --num_targets;
                                }
                            }
                            else
                            {
                                temp_stop_counter[target] = 0;
                            }
                        }
                    }
                }
                /* End flow control */
            }
            else //num_targets == 0
            {
                std::this_thread::sleep_for(std::chrono::nanoseconds(50));
                send_itr_counter = 0;
            }
        }

        spdlog::debug("Wait for request end");

        int num_procs_finished = 0;
        ++num_procs_finished;

        /* Last check if any process still waiting*/
        MPI_Win_lock(MPI_LOCK_EXCLUSIVE, my_world_rank, 0, global_win);
        MPI_Get(get_buf, 2, MPI_UNSIGNED_LONG, my_world_rank, 0, 2, MPI_UNSIGNED_LONG, global_win);

        if (get_buf[0] == REQUEST_FOR_TASKS)
        {
            //notify the stave processe
            ulong send_buf[4];
            send_buf[0] = send_buf[2] = send_buf[3] = 0;
            send_buf[1] = STOP_SIGN_NB_SENDRECV;
            MPI_Request request;

            int target = get_buf[1];

            /* Exit if unexpected target received, indicate errors in code */
            assert(target >= 0 && target < num_world_nodes);

            MPI_Isend(send_buf, 4, MPI_UNSIGNED_LONG, target, send_tags[target], comm, &request);

            spdlog::debug("WIN End sending jobs to {}", target);
        }

        put_buf[0] = put_buf[1] = ALL_LOCAL_TASKS_COMPLETED;

        MPI_Put(put_buf, 2, MPI_UNSIGNED_LONG, my_world_rank, 0, 2, MPI_UNSIGNED_LONG, global_win);
        MPI_Win_flush(my_world_rank, global_win);
        MPI_Win_unlock(my_world_rank, global_win);

        //notify all stave processes
        {
            ulong send_buf[4];
            send_buf[0] = send_buf[2] = send_buf[3] = 0;
            send_buf[1] = STOP_SIGN_NB_SENDRECV;
            MPI_Request request;

            for (int i = 0; i < num_world_nodes; ++i)
            {
                if (starve_proc[i] == 1)
                {
                    MPI_Isend(send_buf, 4, MPI_UNSIGNED_LONG, i, send_tags[i], comm, &request);

                    spdlog::debug("End sending jobs to {}", i);
                }
            }
        }

        /* Check and get jobs from other nodes */

        put_buf[0] = REQUEST_FOR_TASKS;
        put_buf[1] = my_world_rank;

        int is_proc_finished[num_world_nodes];
        for (int i = 0; i < num_world_nodes; ++i)
            is_proc_finished[i] = 0;

        is_proc_finished[my_world_rank] = 1;

        /* Begin in receiving status */
        uint queue_counter = 0;

        while (!(queue[queue_counter]->empty()))
        {
            std::this_thread::sleep_for(std::chrono::nanoseconds(10));
        }

        spdlog::debug("Jobs request begin");

        volatile int parse_queue_status;
        //parse_queue_status = (volatile int *)malloc(sizeof(int));

        parse_queue_status = 0;

        std::list<std::vector<void *> *> *recv_buf_list = new std::list<std::vector<void *> *>();

        std::thread temp_thread(parse_thread_wrapper, num_nb_sendrecv * 2, num_threads, queue, recv_buf_list, &parse_queue_status, is_queue_filled);
        //vect_recv_threads.push_back(std::move(temp_thread));
        temp_thread.detach();

        while (num_procs_finished < num_world_nodes)
        {
            int source = -1;
            int temp_source = -1;
            ulong max_num_tasks = 0;

            for (int i = 0; i < num_world_nodes; ++i)
            {
                temp_source = (my_world_rank + i) % num_world_nodes;

                if (is_proc_finished[temp_source] == 1)
                    continue;

                MPI_Win_lock(MPI_LOCK_EXCLUSIVE, temp_source, 0, global_win);
                MPI_Win_flush(temp_source, global_win);
                MPI_Get(get_buf, 2, MPI_UNSIGNED_LONG, temp_source, 0, 2, MPI_UNSIGNED_LONG, global_win);
                MPI_Win_flush(temp_source, global_win);

                if (get_buf[0] == ALL_LOCAL_TASKS_COMPLETED)
                {
                    MPI_Win_unlock(temp_source, global_win);
                    is_proc_finished[temp_source] = 1;
                    ++num_procs_finished;
                    spdlog::debug("Found {} done", temp_source);
                    continue;
                }

                if (get_buf[0] == REQUEST_FOR_TASKS)
                {
                    /* One starving process is already waiting */
                    /* Skip this victim */
                    MPI_Win_unlock(temp_source, global_win);
                    spdlog::debug("WIN {} collision", temp_source);

                    /* If no target yet, use this temporarily */
                    if (max_num_tasks == 0)
                    {
                        source = temp_source;
                    }
                    continue;
                }

                if (get_buf[0] == RMA_NOT_READY)
                {
                    /* victim not ready yet */
                    /* Skip this victim */
                    MPI_Win_unlock(temp_source, global_win);
                    spdlog::debug("WIN {} not ready", temp_source);

                    continue;
                }

                if (get_buf[0] == WAIT_FOR_TASKS_REQUEST)
                {
                    /* change victim as this one has more tasks */
                    if (source == -1 || get_buf[1] > max_num_tasks)
                    {
                        source = temp_source;
                        max_num_tasks = get_buf[1];
                    }

                    MPI_Win_unlock(temp_source, global_win);
                    continue;
                }

                spdlog::error("Should not reach here {} {}, source {}", get_buf[0], get_buf[1], temp_source);
                MPI_Win_unlock(temp_source, global_win);
            }

            if (num_procs_finished >= num_world_nodes)
            {
                /* All tasks are done on all processes */
                break;
            }

            if (source == -1)
            {
                /* Failed to get a victim, search for another round */
                spdlog::debug("Failed to get a victim, indicates an error");

                for (int k = 0; k < num_world_nodes; ++k)
                    std::cout << is_proc_finished[k] << " " << std::endl;

                break;
            }

            spdlog::debug("Pre-Request tasks from {}", source);
            /* In case the current victim's window is being used*/
            int is_win_occupied = 1;
            while (is_win_occupied)
            {
                MPI_Win_lock(MPI_LOCK_EXCLUSIVE, source, 0, global_win);
                MPI_Win_flush(source, global_win);
                MPI_Get(get_buf, 2, MPI_UNSIGNED_LONG, source, 0, 2, MPI_UNSIGNED_LONG, global_win);
                MPI_Win_flush(source, global_win);

                if (get_buf[0] == REQUEST_FOR_TASKS)
                {
                    MPI_Win_unlock(source, global_win);
                    std::this_thread::sleep_for(std::chrono::nanoseconds(50));
                }
                else if (get_buf[0] == ALL_LOCAL_TASKS_COMPLETED)
                {
                    MPI_Win_unlock(source, global_win);
                    is_proc_finished[source] = 1;
                    ++num_procs_finished;
                    is_win_occupied = 0;
                    spdlog::debug("Pre-Request tasks from {} but its tasks were done", source);
                    source = -1;
                    break;
                }
                else if (get_buf[0] == WAIT_FOR_TASKS_REQUEST)
                {
                    is_win_occupied = 0;
                    MPI_Put(put_buf, 2, MPI_UNSIGNED_LONG, source, 0, 2, MPI_UNSIGNED_LONG, global_win);
                    MPI_Win_flush(source, global_win);
                    MPI_Win_unlock(source, global_win);
                    break;
                }
                else
                {
                    spdlog::error("Should not reach here {} {}, source {}", get_buf[0], get_buf[1], source);
                    MPI_Win_unlock(source, global_win);
                }
            }

            if (source == -1)
            {
                continue;
            }

            volatile int is_all_recv_done = 0;

            spdlog::debug("Request tasks from {} with tag {}, flag {}", source, recv_tags[source], is_all_recv_done);

            while (!is_all_recv_done)
            {
                int num_local_tasks = 0;
                MPI_Request temp_request;

                /* Flow control */
                for (int i = 0; i < num_threads; ++i)
                {
                    num_local_tasks += queue[i]->size();
                }
                num_local_tasks += (recv_buf_list->size() * num_tasks);

                MPI_Isend(&num_local_tasks, 1, MPI_INT, source, ACK_TAG, comm, &temp_request);

                std::vector<std::thread> vect_recv_threads;
                vect_recv_threads.clear();

                for (int i = 0; i < num_nb_sendrecv; ++i)
                {
                    /* Parallel irecv */
                    std::thread temp_thread(recv_tasks, source, recv_tags[source], comm, &is_all_recv_done, recv_buf_list);
                    vect_recv_threads.push_back(std::move(temp_thread));

                    recv_tags[source] += 10;

                    if (recv_tags[source] > tag_ub_val)
                        recv_tags[source] = 0;
                }

                for (int i = 0; i < num_nb_sendrecv; ++i)
                {
                    vect_recv_threads.at(i).join();
                }

                if ((is_all_recv_done % 10) == 1)
                {
                    recv_tags[source] = (is_all_recv_done - 1) + num_nb_sendrecv * 10;

                    spdlog::debug("Temp stop signal received {}. # remain tasks {}", is_all_recv_done, num_local_tasks);
                    //int dummy_int = 0;
                    //MPI_Isend(&dummy_int, 1, MPI_INT, source, ACK_TAG, comm, &temp_request);

                    num_local_tasks = 0;

                    for (int i = 0; i < num_threads; ++i)
                    {
                        num_local_tasks += queue[i]->size();
                    }

                    num_local_tasks += (recv_buf_list->size() * num_tasks);

                    auto t_temp_start = std::chrono::steady_clock::now();
                    while (num_local_tasks > num_tasks * 2)
                    {
                        std::this_thread::sleep_for(std::chrono::nanoseconds(100));
                        num_local_tasks = 0;

                        /* Provide a dummy sync with other win*/
                        MPI_Win_lock(MPI_LOCK_EXCLUSIVE, my_world_rank, 0, global_win);
                        MPI_Win_flush(my_world_rank, global_win);
                        MPI_Win_unlock(my_world_rank, global_win);

                        for (int i = 0; i < num_threads; ++i)
                        {
                            num_local_tasks += queue[i]->size();
                        }

                        num_local_tasks += (recv_buf_list->size() * num_tasks);

                        auto t_temp_end = std::chrono::steady_clock::now();
                        std::chrono::duration<double> t_diff_temp = t_temp_end - t_temp_start;

                        if (t_diff_temp.count() > 1.0)
                        {
                            spdlog::debug("Temp wait # remain tasks {}", num_local_tasks);
                            t_temp_start = std::chrono::steady_clock::now();
                            for (int i = 0; i < num_threads; ++i)
                                std::cout<<queue[i]->size()<<" "<<is_queue_filled[i]<<",";

                            std::cout<<std::endl;
                        }
                    }

                    spdlog::debug("# remain tasks {}", num_local_tasks);
                }
                else if (is_all_recv_done)
                {
                    recv_tags[source] = (is_all_recv_done - 1) + num_nb_sendrecv * 10;
                    is_proc_finished[source] = 1;
                    ++num_procs_finished;

                    num_local_tasks = 0;

                    for (int i = 0; i < num_threads; ++i)
                    {
                        num_local_tasks += queue[i]->size();
                    }

                    num_local_tasks += (recv_buf_list->size() * num_tasks);

                    auto t_temp_start = std::chrono::steady_clock::now();
                    while (num_local_tasks > num_tasks * 2)
                    {
                        std::this_thread::sleep_for(std::chrono::nanoseconds(100));
                        num_local_tasks = 0;

                        /* Provide a dummy sync with other win*/
                        MPI_Win_lock(MPI_LOCK_EXCLUSIVE, my_world_rank, 0, global_win);
                        MPI_Win_flush(my_world_rank, global_win);
                        MPI_Win_unlock(my_world_rank, global_win);

                        for (int i = 0; i < num_threads; ++i)
                        {
                            num_local_tasks += queue[i]->size();
                        }

                        num_local_tasks += (recv_buf_list->size() * num_tasks);

                        auto t_temp_end = std::chrono::steady_clock::now();
                        std::chrono::duration<double> t_diff_temp = t_temp_end - t_temp_start;

                        if (t_diff_temp.count() > 1.0)
                        {
                            spdlog::debug("Last wait # remain tasks {}", num_local_tasks);
                            t_temp_start = std::chrono::steady_clock::now();
                        }
                        //spdlog::debug("# remain tasks {}", num_local_tasks);
                    }

                    spdlog::debug("Post Irecv sign {}, isend target {} val {}", is_all_recv_done, source, num_local_tasks);
                    //MPI_Isend(&num_local_tasks, 1, MPI_INT, source, ACK_TAG, comm, &temp_request);
                }
                else
                {
                    ;
                }
            }

            spdlog::debug("End request jobs from {}", source);
        }

        /* Notify all threads all global jobs are done */
        parse_queue_status = -1;

        spdlog::debug("Global_Tasks_monitor_thread end");

        MPI_Barrier(comm);

        while (parse_queue_status != 0)
        {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        sysinfo(&mem_info);
        current_free_mem = mem_info.freeram;
        current_free_mem *= mem_info.mem_unit;
        spdlog::debug("End free RAM. {} of {}", current_free_mem, total_phys_mem);
        MPI_Win_free(&global_win);
    }

    ulong geoms_type_get(geos::geom::Geometry *geom)
    {
        if (geom == NULL)
            return 0;
        ulong val = 0;

        if (!strcmp(geom->getGeometryType().c_str(), "Polygon"))
            val = 1;
        else if (!strcmp(geom->getGeometryType().c_str(), "LineString"))
            val = 2;
        else if (!strcmp(geom->getGeometryType().c_str(), "Point"))
            val = 3;
        else
        {
            spdlog::error("UNKNOWN GEOM TYPE: {}", geom->getGeometryType());
            val = 0;
        }

        return val;
    }
    //Send four numbers for sending geometries. 0 # of geometries from layer1; 1 size of the array of sizes of geometries from layer1;
    // 2 # of geometries from layer2; 3 size of the array of sizes of geometries from layer2;
    // TODO: geometry types getGeometryTypeId()
    //
    // send_buf_geoms [Pair0.geoms. x,y,x,y....][Pair0.vect. x,y,x,y...][Pair1...
    // send_buf_sizes [sizeof geoms of Pair0][sizeof geom0 in vect of Pair0][sizeof geom1 in vect of Pair0]...

    void MPI_Util_send_buf_gen(std::list<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> *list_send_geoms,
                               ulong *send_buf, double *&send_buf_geoms, ulong *&send_buf_sizes, ulong *&send_map_arr)
    {
        ulong i, j, send_buf_geoms_counter, send_buf_sizes_counter, map_arr_counter;
        //std::unique_ptr<CoordinateSequence> verticesOfGeom;
        //spdlog::info("MPI_Util_send_buf_gen begin");
        assert(send_buf != NULL);

        //ulong size_before_shrink = list_send_geoms->size();
        combine_dupilcate_geoms(list_send_geoms);
        //if (size_before_shrink != list_send_geoms->size())
        //    spdlog::debug("Shrink size from {} to {}", size_before_shrink, list_send_geoms->size());

        send_buf[0] = list_send_geoms->size();
        send_buf[1] = 0;
        send_buf[2] = 0;
        send_buf[3] = 0;

        send_buf_geoms_counter = 0;
        send_buf_sizes_counter = 0;
        map_arr_counter = 0;

        send_map_arr = (ulong *)malloc(send_buf[0] * sizeof(ulong));

        assert(send_map_arr != NULL);

        for (std::list<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *>::iterator itr = list_send_geoms->begin();
             itr != list_send_geoms->end(); ++itr)
        {
            std::vector<geos::geom::Coordinate> vect_coords_1;

            send_buf_geoms_counter = send_buf[1] + send_buf[3];

            std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair = *itr;

            /*
            std::cout<<"SEND g1 ";
        	std::cout<<temp_pair->first->toString()<<std::endl;
        	std::cout<<"SEND g2"<<std::endl;
        	for (uint k = 0; k < temp_pair->second->size(); ++k)
        	{
        		std::cout<<temp_pair->second->at(k)->toString()<<std::endl;
        	}
            */

            temp_pair->first->getCoordinates()->toVector(vect_coords_1);

            send_buf[1] += vect_coords_1.size();
            send_buf[2] += temp_pair->second->size();
            send_map_arr[map_arr_counter] = temp_pair->second->size();
            ++map_arr_counter;

            send_buf_geoms = (double *)realloc(send_buf_geoms,
                                               (send_buf_geoms_counter + vect_coords_1.size()) * 2 * sizeof(double));

            assert(send_buf_geoms != NULL);

            for (i = 0; i < vect_coords_1.size(); ++i)
            {
                send_buf_geoms[2 * send_buf_geoms_counter + 2 * i] = vect_coords_1[i].x;
                send_buf_geoms[2 * send_buf_geoms_counter + 2 * i + 1] = vect_coords_1[i].y;
            }

            send_buf_geoms_counter += vect_coords_1.size();

            send_buf_sizes = (ulong *)realloc(send_buf_sizes,
                                              2 * (send_buf_sizes_counter + 1 + temp_pair->second->size()) * sizeof(ulong));

            assert(send_buf_sizes != NULL);

            send_buf_sizes[2 * send_buf_sizes_counter] = vect_coords_1.size();
            send_buf_sizes[2 * send_buf_sizes_counter + 1] = geoms_type_get(temp_pair->first);

            j = 1;

            for (std::vector<geos::geom::Geometry *>::iterator inner_itr = temp_pair->second->begin();
                 inner_itr != temp_pair->second->end(); ++inner_itr)
            {
                std::vector<geos::geom::Coordinate> vect_coords_2;
                (*inner_itr)->getCoordinates()->toVector(vect_coords_2);
                send_buf[3] += vect_coords_2.size();

                send_buf_geoms = (double *)realloc(send_buf_geoms,
                                                   (send_buf_geoms_counter + vect_coords_2.size()) * 2 * sizeof(double));

                assert(send_buf_geoms);

                for (i = 0; i < vect_coords_2.size(); ++i)
                {
                    send_buf_geoms[2 * send_buf_geoms_counter + 2 * i] = vect_coords_2[i].x;
                    send_buf_geoms[2 * send_buf_geoms_counter + 2 * i + 1] = vect_coords_2[i].y;
                }

                send_buf_geoms_counter += vect_coords_2.size();

                send_buf_sizes[2 * (send_buf_sizes_counter + j)] = vect_coords_2.size();
                send_buf_sizes[2 * (send_buf_sizes_counter + j) + 1] = geoms_type_get((*inner_itr));
                ++j;
            }

            send_buf_sizes_counter += (1 + temp_pair->second->size());
        }

        //spdlog::info("{} {} {} {} {} {} {} ", send_buf[0], send_buf[1], send_buf[2], send_buf[3],
        //             send_buf_geoms_counter, send_buf_sizes_counter,
        //             send_buf[0]);
        //spdlog::info("MPI_Util_send_buf_gen end ");
    }

    //Recv four numbers for sending geometries. 0 # of geometries from layer1; 1 size of the array of sizes of geometries from layer1;
    // 2 # of geometries from layer2; 3 size of the array of sizes of geometries from layer2;
    // TODO: geometry types getGeometryTypeId()
    //
    // recv_buf_geoms [Pair0.geoms. x,y,x,y....][Pair0.vect. x,y,x,y...][Pair1...
    // recv_buf_sizes [sizeof geoms of Pair0][sizeof geom0 in vect of Pair0][sizeof geom1 in vect of Pair0]...
    // 			MPI_Recv(recv_buf_geoms, (recv_buf[1] + recv_buf[3])*2,
    //				MPI_DOUBLE, target, myWorldRank, MPI_COMM_WORLD, &status);
    //			MPI_Recv(recv_buf_sizes, recv_buf[0] + recv_buf[2],
    //				MPI_UNSIGNED_LONG, target, myWorldRank + 1, MPI_COMM_WORLD, &status);

    void MPI_Util_parse_recv_buf(WorkStealingQueue<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> *queue,
                                 ulong *recv_buf, double *recv_buf_geoms, ulong *recv_buf_sizes, ulong *recv_map_arr, std::mutex *thread_mutex)
    {
        //spdlog::info("MPI_Util_parse_recv_buf begin {} {} {} {}", recv_buf[0], recv_buf[1], recv_buf[2], recv_buf[3]);

        ulong i, j, num_geoms, size_geom_1, size_geom_2, num_map_geoms, map_arr_counter, geoms_arr_start;
        geos::geom::Geometry *geom1, *geom2;
        std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *temp_pair = NULL;
        geos::geom::GeometryFactory::Ptr gf = geos::geom::GeometryFactory::create();

        if (queue == NULL || recv_buf == NULL || recv_buf_geoms == NULL || recv_buf_sizes == NULL || recv_map_arr == NULL)
        {
            spdlog::error("MPI_Util_parse_recv_buf get NULL input");
            return;
        }

        num_geoms = recv_buf[0] + recv_buf[2];

        i = j = map_arr_counter = geoms_arr_start = 0;

        for (i = 0; i < num_geoms;)
        {
            //std::cout<<"1"<<std::endl;
            size_geom_1 = recv_buf_sizes[2 * i];
            ulong geom_type = recv_buf_sizes[2 * i + 1];

            num_map_geoms = recv_map_arr[map_arr_counter];
            ++map_arr_counter;

            double *geom_arr_1 = (double *)malloc(size_geom_1 * 2 * sizeof(double));
            assert(geom_arr_1);

            memcpy(geom_arr_1, recv_buf_geoms + geoms_arr_start * 2, size_geom_1 * 2 * sizeof(double));
            geom1 = arr_to_geoms(geom_arr_1, size_geom_1, gf->getDefaultInstance(), geom_type);
            assert(geom1);

            free(geom_arr_1);

            temp_pair = new std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *>();
            temp_pair->first = geom1;

            geoms_arr_start += size_geom_1;

            temp_pair->second = new std::vector<geos::geom::Geometry *>();

            for (j = 0; j < num_map_geoms; ++j)
            {
                size_geom_2 = recv_buf_sizes[2 * (i + j + 1)];
                geom_type = recv_buf_sizes[2 * (i + j + 1) + 1];

                double *geom_arr_2 = (double *)malloc(size_geom_2 * 2 * sizeof(double));
                assert(geom_arr_2);

                memcpy(geom_arr_2, recv_buf_geoms + geoms_arr_start * 2, size_geom_2 * 2 * sizeof(double));
                geoms_arr_start += size_geom_2;

                geom2 = arr_to_geoms(geom_arr_2, size_geom_2, gf->getDefaultInstance(), geom_type);
                assert(geom2);
                free(geom_arr_2);

                temp_pair->second->push_back(geom2);
            }

            i += (1 + num_map_geoms);

            thread_mutex->lock();

            if (temp_pair->second->size() <= TASKS_PER_JOB)
            {
                queue->push(temp_pair);
            }
            else
            {
                uint vect_pos = 0;
                std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *inner_temp_pair = NULL;
                while (vect_pos < temp_pair->second->size())
                {
                    uint temp_counter = 0;
                    inner_temp_pair = new std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *>();
                    inner_temp_pair->first = temp_pair->first->clone().release();
                    //inner_temp_pair->first = temp_pair->first;
                    inner_temp_pair->second = new std::vector<geos::geom::Geometry *>();

                    while (temp_counter < TASKS_PER_JOB && vect_pos < temp_pair->second->size())
                    {
                        inner_temp_pair->second->push_back(temp_pair->second->at(vect_pos));
                        ++vect_pos;
                        ++temp_counter;
                    }
                    queue->push(inner_temp_pair);
                }

                temp_pair->first->~Geometry();
                delete temp_pair;
            }

            thread_mutex->unlock();

            /*
            std::cout<<"RECV g1 ";
        	std::cout<<temp_pair->first->toString()<<std::endl;
        	std::cout<<"RECV g2"<<std::endl;
        	for (uint k = 0; k < temp_pair->second->size(); ++k)
        	{
        		std::cout<<temp_pair->second->at(k)->toString()<<std::endl;
        	}
            */
        }

        free(recv_buf);
        free(recv_buf_geoms);
        free(recv_buf_sizes);
        free(recv_map_arr);
        //spdlog::info("MPI_Util_parse_recv_buf end");
    }

    geos::geom::Geometry *arr_to_geoms(double *geomArray, ulong numVertices, const geos::geom::GeometryFactory *gf, ulong geom_type)
    {
        //spdlog::info("arr_to_geoms begin {}", geom_type);
        geos::geom::Geometry *geo = NULL;
        geos::geom::CoordinateArraySequence *coords_arr;
        geos::geom::CoordinateArraySequenceFactory csf;

        coords_arr = new geos::geom::CoordinateArraySequence(numVertices);
        for (ulong i = 0; i < numVertices; ++i)
        {
            coords_arr->setAt(geos::geom::Coordinate(geomArray[2 * i], geomArray[2 * i + 1]), i);
        }

        //const geos::geom::CoordinateArraySequence const_coords_arr(*coords_arr);

        if (geom_type == 1)
        {
            //POLYGON
            try
            {
                // Create non-empty LinearRing instance
                geos::geom::LinearRing ring(coords_arr, gf);
                // Exterior (clone is required here because Polygon takes ownership)
                geo = ring.clone().release();

                geos::geom::LinearRing *exterior = dynamic_cast<geos::geom::LinearRing *>(geo);
                std::unique_ptr<geos::geom::Polygon> poly(gf->createPolygon(exterior, nullptr));

                //geos::geom::LinearRing* ring = new geos::geom::LinearRing(coords_arr, gf);
                //std::unique_ptr<geos::geom::Polygon> poly(gf->createPolygon(ring, nullptr));
                geo = dynamic_cast<geos::geom::Geometry *>(poly.release());

                //delete coords_arr;
            }
            catch (std::exception &e)
            {
                spdlog::error("{}", e.what());
            }
        }
        else if (geom_type == 2)
        {
            //LINESTRING
            try
            {
                std::unique_ptr<geos::geom::LineString> line(gf->createLineString(coords_arr));

                geo = dynamic_cast<geos::geom::Geometry *>(line.release());
            }
            catch (std::exception &e)
            {
                spdlog::error("{}", e.what());
            }
        }
        else if (geom_type == 3)
        {
            //POINT
            try
            {
                std::unique_ptr<geos::geom::Point> point(gf->createPoint(coords_arr));
                geo = dynamic_cast<geos::geom::Geometry *>(point.release());
            }
            catch (std::exception &e)
            {
                spdlog::error("{}", e.what());
            }
        }
        else
        {
            //Unkown type, try to convert to polygon
            spdlog::error("Parsing Unknown type: {}", geom_type);

            try
            {
                // Create non-empty LinearRing instance
                geos::geom::LinearRing ring(coords_arr, gf);
                // Exterior (clone is required here because Polygon takes ownership)
                geo = ring.clone().release();

                geos::geom::LinearRing *exterior = dynamic_cast<geos::geom::LinearRing *>(geo);
                std::unique_ptr<geos::geom::Polygon> poly(gf->createPolygon(exterior, nullptr));

                //geos::geom::LinearRing* ring = new geos::geom::LinearRing(coords_arr, gf);
                //std::unique_ptr<geos::geom::Polygon> poly(gf->createPolygon(ring, nullptr));
                geo = dynamic_cast<geos::geom::Geometry *>(poly.release());
            }
            catch (std::exception &e)
            {
                spdlog::error("{}", e.what());
            }
        }

        //spdlog::info("arr_to_geoms end");
        if (geo == NULL)
        {
            spdlog::error("Parsing to NULL GEO: {}", geom_type);

#ifdef DEBUG
            for (ulong k = 0; k < 2 * numVertices; ++k)
                printf("%3.16f ", geomArray[k]);

            printf("/n");
#endif //ifdef DEBUG
        }
        else
        {
            //std::cout<<numVertices << " "<<geo->toString()<<std::endl;
        }

        //spdlog::debug("arr_to_geoms end {}", geom_type);
        return geo;
    }

} //namespace gsj

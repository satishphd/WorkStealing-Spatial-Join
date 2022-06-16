#define USE_UNSTABLE_GEOS_CPP_API
#include <global.h>

void set_numa_policy(void);
int get_divisor(int num_world_nodes);
int test(int argc, char **argv);
int test_unpartition(int argc, char **argv);

int main(int argc, char **argv)
{
    //test(argc, argv);
    test_unpartition(argc, argv);
    return 0;
}

int test(int argc, char **argv)
{
    int num_world_nodes, my_world_rank;

    //MPI_Init(&argc, &argv );
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_world_nodes);

    spdlog::set_pattern("P" + std::to_string(my_world_rank) + " [%H:%M:%S.%e] %v");

#ifdef DEBUG
    spdlog::set_level(spdlog::level::debug); // Set global log level to debug
#else
    spdlog::set_level(spdlog::level::info); // Set global log level to info
#endif

    char node_name[MPI_MAX_PROCESSOR_NAME];
    int len;
    MPI_Get_processor_name(node_name, &len);

    spdlog::info("Node name {} rank {} pid {}", node_name, my_world_rank, getpid());

    const int num_threads = std::stoi(argv[1]);
    const int num_nb_sendrecv = std::stoi(argv[2]);
    const int num_tasks_to_send = std::stoi(argv[3]);
    const int num_files = std::stoi(argv[4]);
    const std::string file_path_1 = argv[5];
    const std::string file_path_2 = argv[6];

    if (my_world_rank == 0)
    {
        spdlog::info("Num_threads {}; num_nb_sendrecv {}; num_tasks per sendrecv {}; num files {}",
                     num_threads, num_nb_sendrecv, num_tasks_to_send, num_files);

        spdlog::info("FILE PATH1 {} :: FILE PATH2{}", file_path_1, file_path_2);

#ifdef USE_ST_INTERSECTION
        spdlog::info("Spatial join operation {}", "USE_ST_INTERSECTION");

#elif USE_ST_INTERSECTS
        spdlog::info("Spatial join operation {}", "USE_ST_INTERSECTS");

#elif USE_ST_UNION
        spdlog::info("Spatial join operation {}", "USE_ST_UNION");

#else //default using USE_ST_INTERSECTION
        spdlog::info("Spatial join operation {}", "USE_ST_INTERSECTION");
#endif
    }

    set_numa_policy();

    std::vector<std::list<std::pair<geos::geom::Geometry *, int> *> *> *vect_list_geoms_1 = new std::vector<std::list<std::pair<geos::geom::Geometry *, int> *> *>(num_threads);
    std::vector<std::list<std::pair<geos::geom::Geometry *, int> *> *> *vect_list_geoms_2 = new std::vector<std::list<std::pair<geos::geom::Geometry *, int> *> *>(num_threads);

    for (int i = 0; i < num_threads; ++i)
    {
        vect_list_geoms_1->at(i) = new std::list<std::pair<geos::geom::Geometry *, int> *>();
        vect_list_geoms_2->at(i) = new std::list<std::pair<geos::geom::Geometry *, int> *>();
    }

    gsj::Reader rd;
    int file_name_start = my_world_rank * num_files / num_world_nodes;
    int file_name_end = (my_world_rank + 1) * num_files / num_world_nodes;

    for (int i = file_name_start; i < file_name_end; ++i)
    {
        std::list<geos::geom::Geometry *> *temp_list_geoms_1 = new std::list<geos::geom::Geometry *>();
        std::list<geos::geom::Geometry *> *temp_list_geoms_2 = new std::list<geos::geom::Geometry *>();

        std::string temp_file_path1 = file_path_1 + std::to_string(i);
        std::string temp_file_path2 = file_path_2 + std::to_string(i);

        //rd.Read_Geoms_from_file_parallel(temp_file_path1, vect_list_geoms_1->at(i % num_threads), num_threads);
        //rd.Read_Geoms_from_file_parallel(temp_file_path2, vect_list_geoms_2->at(i % num_threads), num_threads);

        rd.Read_Geoms_from_file_parallel(temp_file_path1, temp_list_geoms_1, num_threads);
        rd.Read_Geoms_from_file_parallel(temp_file_path2, temp_list_geoms_2, num_threads);

        for (std::list<geos::geom::Geometry *>::iterator itr = temp_list_geoms_1->begin(); itr != temp_list_geoms_1->end(); ++itr)
        {
            vect_list_geoms_1->at(i % num_threads)->push_back(new std::pair<geos::geom::Geometry *, int>(*itr, i));
        }

        for (std::list<geos::geom::Geometry *>::iterator itr = temp_list_geoms_2->begin(); itr != temp_list_geoms_2->end(); ++itr)
        {
            vect_list_geoms_2->at(i % num_threads)->push_back(new std::pair<geos::geom::Geometry *, int>(*itr, i));
        }

        delete temp_list_geoms_1;
        delete temp_list_geoms_2;
    }

    MPI_Barrier(MPI_COMM_WORLD);
    spdlog::debug("Parsing finished");

    ulong join_result = gsj::Thread_Wrapper_MPI_spatial_join_pre_parsing(num_threads, num_nb_sendrecv, num_tasks_to_send, vect_list_geoms_1, vect_list_geoms_2);
    ulong global_join_result = 0;

    MPI_Allreduce(&join_result, &global_join_result, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

    spdlog::info("Main thread end with join result = {} : {}", join_result, global_join_result);

    MPI_Finalize();
    return 0;
}

void set_numa_policy(void)
{
#ifdef NUMA_ENABLED
    bitmask *current_bitmask = numa_get_membind();

    //printf("%d %d %d %ld \n", thread_rank, current_cpu_id, numa_node, *(current_bitmask->maskp));

    bitmask new_bitmask;
    new_bitmask.size = current_bitmask->size;
    new_bitmask.maskp = (ulong *)malloc(new_bitmask.size);

    for (int i = 0; i < (int)(new_bitmask.size / 8); ++i)
    {
        new_bitmask.maskp[i] = 0;
    }

    if (numa_preferred())
    {
        *(new_bitmask.maskp) = 3;
        //*(new_bitmask.maskp+1) = 1;
    }
    else
    {
        *(new_bitmask.maskp) = 3;
        //*(new_bitmask.maskp+1) = 2;
    }

    //numa_bind(&new_bitmask);
    set_mempolicy(MPOL_INTERLEAVE, new_bitmask.maskp, new_bitmask.size);
//set_mempolicy(MPOL_BIND, new_bitmask.maskp,  new_bitmask.size);
//set_mempolicy(MPOL_DEFAULT , NULL,  0);
#endif
}

int test_unpartition(int argc, char **argv)
{
    int num_world_nodes, my_world_rank;

    //MPI_Init(&argc, &argv );
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_world_nodes);

    spdlog::set_pattern("P" + std::to_string(my_world_rank) + " [%H:%M:%S.%e] %v");

#ifdef DEBUG
    spdlog::set_level(spdlog::level::debug); // Set global log level to debug
#else
    spdlog::set_level(spdlog::level::info); // Set global log level to info
#endif

    char node_name[MPI_MAX_PROCESSOR_NAME];
    int len;
    MPI_Get_processor_name(node_name, &len);

    spdlog::info("Node name {} rank {} pid {}", node_name, my_world_rank, getpid());

    const int num_threads = std::stoi(argv[1]);
    const int num_nb_sendrecv = std::stoi(argv[2]);
    const int num_tasks_to_send = std::stoi(argv[3]);
    const std::string file_path_1 = argv[4];
    const std::string file_path_2 = argv[5];

    int divisor_1 = get_divisor(num_world_nodes);
    int divisor_2 = num_world_nodes / divisor_1;

    assert(divisor_1 >= 1 && divisor_1 <= num_world_nodes);
    assert(divisor_2 >= 1 && divisor_2 <= num_world_nodes);
    assert(divisor_1 * divisor_2 == num_world_nodes);

    if (my_world_rank == 0)
    {
        spdlog::info("Num_threads {}; num_nb_sendrecv {}; num_tasks per sendrecv {}",
                     num_threads, num_nb_sendrecv, num_tasks_to_send);

        spdlog::info("FILE PATH1 {} :: FILE PATH2{}", file_path_1, file_path_2);

#ifdef USE_ST_INTERSECTION
        spdlog::info("Spatial join operation {}", "USE_ST_INTERSECTION");

#elif USE_ST_INTERSECTS
        spdlog::info("Spatial join operation {}", "USE_ST_INTERSECTS");

#elif USE_ST_UNION
        spdlog::info("Spatial join operation {}", "USE_ST_UNION");

#else //default using USE_ST_INTERSECTION
        spdlog::info("Spatial join operation {}", "USE_ST_INTERSECTION");
#endif
    }

    set_numa_policy();

    std::vector<std::string> *vect_strs_1 = new std::vector<std::string>();
    std::vector<std::string> *vect_strs_2 = new std::vector<std::string>();

    std::vector<std::string> *vect_strs_1_temp = new std::vector<std::string>();
    std::vector<std::string> *vect_strs_2_temp = new std::vector<std::string>();

    std::list<geos::geom::Geometry *> *list_geoms_1 = new std::list<geos::geom::Geometry *>();
    std::list<geos::geom::Geometry *> *list_geoms_2 = new std::list<geos::geom::Geometry *>();
    std::list<std::pair<geos::geom::Geometry *, geos::geom::Envelope *> *> *list_geoms_2_w_envs =
        new std::list<std::pair<geos::geom::Geometry *, geos::geom::Envelope *> *>();

    gsj::Reader rd;

    //rd.Reader_Load_part_file(file_path_1, vect_strs_1, divisor_1, MPI_COMM_WORLD);

    rd.Read_Strs_from_file(file_path_1, vect_strs_1_temp);

    ulong num_strs_vect_1 = vect_strs_1_temp->size() / divisor_1;
    ulong sub_vect_start_1 = num_strs_vect_1 * (my_world_rank % divisor_1);
    ulong sub_vect_end_1 = sub_vect_start_1 + num_strs_vect_1;
    if ((divisor_1 - 1) == (my_world_rank % divisor_1))
        sub_vect_end_1 = vect_strs_1_temp->size();

    for (ulong i = sub_vect_start_1; i < sub_vect_end_1; ++i)
    {
        vect_strs_1->push_back(vect_strs_1_temp->at(i));
    }

    spdlog::debug("V1 {}", vect_strs_1->size());
    rd.Read_Geoms_from_strs_parallel(vect_strs_1, list_geoms_1, num_threads);

    vect_strs_1_temp->clear();
    vect_strs_1_temp->shrink_to_fit();
    delete vect_strs_1_temp;
    //rd.Reader_Load_part_file(file_path_2, vect_strs_2, divisor_2, MPI_COMM_WORLD);

    rd.Read_Strs_from_file(file_path_2, vect_strs_2_temp);

    ulong num_strs_vect_2 = vect_strs_2_temp->size() / divisor_2;
    int consecutive_nodes = num_world_nodes / divisor_2;
    ulong sub_vect_start_2 = num_strs_vect_2 * (my_world_rank / consecutive_nodes);
    ulong sub_vect_end_2 = sub_vect_start_2 + num_strs_vect_2;
    if ((divisor_2 - 1) == (my_world_rank / consecutive_nodes))
        sub_vect_end_2 = vect_strs_2_temp->size();

    for (ulong i = sub_vect_start_2; i < sub_vect_end_2; ++i)
    {
        vect_strs_2->push_back(vect_strs_2_temp->at(i));
    }

    spdlog::debug("V2 {}", vect_strs_2->size());
    rd.Read_Geoms_from_strs_parallel(vect_strs_2, list_geoms_2, num_threads);

    vect_strs_2_temp->clear();
    vect_strs_2_temp->shrink_to_fit();
    delete vect_strs_2_temp;

    spdlog::debug("L1 {}  L2 {}", list_geoms_1->size(), list_geoms_2->size());

    std::vector<std::list<geos::geom::Geometry *> *> *vect_list_geoms_1 = new std::vector<std::list<geos::geom::Geometry *> *>(num_threads);

    ulong size_sub_list = list_geoms_1->size() / num_threads;

    for (int i = 0; i < num_threads; ++i)
    {
        vect_list_geoms_1->at(i) = new std::list<geos::geom::Geometry *>();

        while ((!list_geoms_1->empty()) && (vect_list_geoms_1->at(i)->size() < size_sub_list))
        {
            vect_list_geoms_1->at(i)->push_back(list_geoms_1->back());
            list_geoms_1->pop_back();
        }
    }

    while (!list_geoms_1->empty())
    {
        vect_list_geoms_1->at(num_threads - 1)->push_back(list_geoms_1->back());
        list_geoms_1->pop_back();
    }

    for (std::list<geos::geom::Geometry *>::iterator itr = list_geoms_2->begin(); itr != list_geoms_2->end(); ++itr)
    {
        geos::geom::Envelope *temp_env = new geos::geom::Envelope(*((*itr)->getEnvelopeInternal()));

        assert((*itr)->getEnvelopeInternal()->equals(temp_env));
        list_geoms_2_w_envs->push_back(new std::pair<geos::geom::Geometry *, geos::geom::Envelope *>(*itr, temp_env));
    }

    MPI_Barrier(MPI_COMM_WORLD);
    spdlog::debug("Parsing finished");

    spdlog::debug("L1 {}  L2 {}", list_geoms_1->size(), list_geoms_2_w_envs->size());

    ulong join_result = gsj::Thread_Wrapper_MPI_unpartition(num_threads, num_nb_sendrecv, num_tasks_to_send, vect_list_geoms_1, list_geoms_2_w_envs);

    ulong global_join_result = 0;

    MPI_Allreduce(&join_result, &global_join_result, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

    spdlog::info("Main thread end with join result = {} : {}", join_result, global_join_result);

    MPI_Finalize();
    return 0;
}

/* Use a dummy way to get a divisor for unpartitioned data*/
int get_divisor(int num_world_nodes)
{
    int return_val = 0;

    //assert(num_world_nodes > 1 && num_world_nodes < 31);

    for (int i = 1; i < num_world_nodes; ++i)
    {
        int dummy = num_world_nodes / i;

        if ((dummy * i == num_world_nodes) && (i > return_val))
            return_val = i;
    }

    return return_val;
}

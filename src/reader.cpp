#include <reader.h>

namespace gsj
{

    void Reader ::Read_Strs_from_file(const std::string file_path, std::vector<std::string> *vect_strs)
    {
        std::ifstream file(file_path.c_str());
        std::string temp_str;

        while (std::getline(file, temp_str))
        {
            //omit empty strings and invalid strings
            if (temp_str.size() > 5)
                vect_strs->push_back(temp_str);
        }

        file.close();
    }

    void Reader ::Read_Geoms_from_strs(std::vector<std::string> *vect_strs, std::list<geos::geom::Geometry *> *list_geoms)
    {
        geos::io::WKTReader reader;
        std::string temp_str;
        geos::geom::Geometry *temp_geom = NULL;
        uint i;

        for (std::vector<std::string>::iterator itr = vect_strs->begin(); itr != vect_strs->end(); ++itr)
        {
            temp_str = *itr;
            temp_geom = NULL;

            try
            {
                temp_geom = (reader.read(temp_str)).release();
            }
            catch (std::exception &e)
            {
                //throw;
            }

            if (temp_geom != NULL)
            { // && temp_geom->isValid()){
                if (temp_geom->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON || temp_geom->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_MULTILINESTRING || temp_geom->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION)
                {
                    for (i = 0; i < temp_geom->getNumGeometries(); ++i)
                    {
                        list_geoms->push_back(const_cast<geos::geom::Geometry *>(temp_geom->getGeometryN(i)));
                    }
                }
                else
                {
                    list_geoms->push_back(temp_geom);
                }
            }
        }

        //Do not free temp_geom, it's used in the future.
    }

    void Reader ::Read_Geoms_from_strs_wkb(std::vector<std::string> *vect_strs, std::list<geos::geom::Geometry *> *list_geoms)
    {
        geos::io::WKBReader reader;
        std::string temp_str;
        geos::geom::Geometry *temp_geom = NULL;
        uint i;

        for (std::vector<std::string>::iterator itr = vect_strs->begin(); itr != vect_strs->end(); ++itr)
        {
            temp_str = *itr;
            temp_geom = NULL;
            std::stringstream temp_stream(temp_str);

            try
            {
                temp_geom = (reader.readHEX(temp_stream)).release();
            }
            catch (std::exception &e)
            {
                //throw;
            }

            if (temp_geom != NULL)
            { // && temp_geom->isValid()){
                if (temp_geom->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON || temp_geom->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_MULTILINESTRING || temp_geom->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION)
                {
                    for (i = 0; i < temp_geom->getNumGeometries(); ++i)
                    {
                        list_geoms->push_back(const_cast<geos::geom::Geometry *>(temp_geom->getGeometryN(i)));
                    }
                }
                else
                {
                    list_geoms->push_back(temp_geom);
                }
            }
        }

        //Do not free temp_geom, it's used in the future.
    }

    static void Thread_Read_geoms_from_strs(std::vector<std::string> *vect_strs, std::list<geos::geom::Geometry *> *list_geoms,
                                            geos::io::WKTReader *reader, std::mutex *push_mutex)
    {
        std::string temp_str;
        geos::geom::Geometry *temp_geom = NULL;
        std::list<geos::geom::Geometry *> *thread_list_geoms = new std::list<geos::geom::Geometry *>();
        uint i;

        for (std::vector<std::string>::iterator itr = vect_strs->begin(); itr != vect_strs->end(); ++itr)
        {
            temp_str = *itr;
            temp_geom = NULL;

            try
            {
                temp_geom = (reader->read(temp_str)).release();
            }
            catch (std::exception &e)
            {
                //throw;
            }

            if (temp_geom != NULL && temp_geom->isValid())
            { // && temp_geom->isValid()){//Disable validation check increase performance
                if (temp_geom->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON || temp_geom->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_MULTILINESTRING || temp_geom->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION)
                {
                    for (i = 0; i < temp_geom->getNumGeometries(); ++i)
                    {
                        thread_list_geoms->push_back(const_cast<geos::geom::Geometry *>(temp_geom->getGeometryN(i)));
                    }
                }
                else
                {
                    thread_list_geoms->push_back(temp_geom);
                }
            }
        }

        push_mutex->lock();

        while (!thread_list_geoms->empty())
        {
            list_geoms->push_back(thread_list_geoms->back());
            thread_list_geoms->pop_back();
        }

        push_mutex->unlock();

        //Do not free temp_geom, it's used in the future.
        //To avoid memory leak, we may free it after using.
    }

    void Reader ::Read_Geoms_from_file(const std::string file_path, std::list<geos::geom::Geometry *> *list_geoms)
    {
        std::vector<std::string> *vect_strs = new std::vector<std::string>;

        Read_Strs_from_file(file_path, vect_strs);

        if (vect_strs->empty())
        {
            //std::cerr << "File [" + file_path + "] is empty." << std::endl;
            return;
        }

        Read_Geoms_from_strs(vect_strs, list_geoms);

        vect_strs->clear();
        vect_strs->shrink_to_fit();
    }

    void Reader ::Read_Geoms_from_file_wkb(const std::string file_path, std::list<geos::geom::Geometry *> *list_geoms)
    {
        std::vector<std::string> *vect_strs = new std::vector<std::string>;

        Read_Strs_from_file(file_path, vect_strs);

        if (vect_strs->empty())
        {
            std::cerr << "File [" + file_path + "] is empty." << std::endl;
            return;
        }

        Read_Geoms_from_strs_wkb(vect_strs, list_geoms);

        vect_strs->clear();
        vect_strs->shrink_to_fit();
    }

    void Reader ::Read_Geoms_from_file_parallel(const std::string file_path, std::list<geos::geom::Geometry *> *list_geoms,
                                                uint num_threads)
    {
        uint i, vect_size;
        geos::io::WKTReader wkt_reader[num_threads];
        std::vector<std::string> *vect_strs = new std::vector<std::string>;
        std::vector<std::string>::iterator sub_itr_begin, sub_itr_end;
        std::vector<std::thread> *local_thread_vect = new std::vector<std::thread>();

        Read_Strs_from_file(file_path, vect_strs);

        if (vect_strs->empty())
            return;

        if (vect_strs->size() < 200) //Too tiny for parallel parsing
        {
            Read_Geoms_from_strs(vect_strs, list_geoms);
            return;
        }
        std::vector<std::vector<std::string> *> vect_l_str(num_threads, NULL);
        vect_size = vect_strs->size();
        vect_size = vect_size / num_threads;

        std::mutex push_mutex;

        for (i = 0; i < num_threads; ++i)
        {
            sub_itr_begin = vect_strs->begin() + i * vect_size;
            if (i == num_threads - 1)
                sub_itr_end = vect_strs->end();
            else
                sub_itr_end = vect_strs->begin() + (i + 1) * vect_size;

            vect_l_str[i] = new std::vector<std::string>(sub_itr_begin, sub_itr_end);
            wkt_reader[i] = geos::io::WKTReader();

            local_thread_vect->push_back(std::thread(Thread_Read_geoms_from_strs,
                                                     vect_l_str[i], list_geoms, &wkt_reader[i], &push_mutex));
        }

        for (std::vector<std::thread>::iterator itr = local_thread_vect->begin(); itr != local_thread_vect->end(); ++itr)
        {
            (*itr).join();
        }

        for (i = 0; i < num_threads; ++i)
            delete vect_l_str[i];

        local_thread_vect->clear();
        local_thread_vect->shrink_to_fit();
        delete local_thread_vect;

        vect_strs->clear();
        vect_strs->shrink_to_fit();
        delete vect_strs;
    }

    void Reader ::Read_Geoms_from_strs_parallel(std::vector<std::string> *vect_strs, std::list<geos::geom::Geometry *> *list_geoms,
                                                uint num_threads)
    {
        uint i, vect_size;
        geos::io::WKTReader wkt_reader[num_threads];
        std::vector<std::string>::iterator sub_itr_begin, sub_itr_end;
        std::vector<std::thread> *local_thread_vect = new std::vector<std::thread>();

        if (vect_strs->empty())
            return;

        if (vect_strs->size() < 200) //Too tiny for parallel parsing
        {
            Read_Geoms_from_strs(vect_strs, list_geoms);
            return;
        }

        std::vector<std::vector<std::string> *> vect_l_str(num_threads, NULL);
        vect_size = vect_strs->size();
        vect_size = vect_size / num_threads;

        std::mutex push_mutex;

        for (i = 0; i < num_threads; ++i)
        {
            sub_itr_begin = vect_strs->begin() + i * vect_size;
            if (i == num_threads - 1)
                sub_itr_end = vect_strs->end();
            else
                sub_itr_end = vect_strs->begin() + (i + 1) * vect_size;

            vect_l_str[i] = new std::vector<std::string>(sub_itr_begin, sub_itr_end);
            wkt_reader[i] = geos::io::WKTReader();

            local_thread_vect->push_back(std::thread(Thread_Read_geoms_from_strs,
                                                     vect_l_str[i], list_geoms, &wkt_reader[i], &push_mutex));
        }

        for (std::vector<std::thread>::iterator itr = local_thread_vect->begin(); itr != local_thread_vect->end(); ++itr)
        {
            (*itr).join();
        }

        for (i = 0; i < num_threads; ++i)
            delete vect_l_str[i];

        local_thread_vect->clear();
        local_thread_vect->shrink_to_fit();
        delete local_thread_vect;

        vect_strs->clear();
        vect_strs->shrink_to_fit();
        delete vect_strs;
    }

    void Reader ::Read_Envs_from_strs(std::vector<std::string> *vect_strs, std::vector<geos::geom::Envelope *> *vect_envs)
    {
        size_t i, j, k, start, end, str_len, size = vect_strs->size();
        std::string temp_str;
        double env[4] = {0.0};
        ;

        for (i = 0; i < size; ++i)
        {
            temp_str = vect_strs->at(i);
            start = end = 0;
            str_len = temp_str.size();

            for (j = 0; j < str_len; ++j)
            {
                k = 0;
                if (temp_str[j] == ' ' && k < 4)
                {
                    end = j;
                    env[k] = std::strtod((temp_str.substr(start, end - start)).c_str(), NULL);
                    ++k;
                    start = j + 1;
                }
                if (4 <= k)
                    break;
            }
            vect_envs->push_back(new geos::geom::Envelope(env[0], env[1], env[2], env[3]));
        }
    }

    void Reader ::Read_Envs_Weights_from_strs(std::vector<std::string> *vect_strs,
                                              std::vector<std::pair<geos::geom::Envelope *, int> *> *vect_envs)
    {
        uint i, j, k, start, end, str_len, size = vect_strs->size();
        std::string temp_str;
        double env[4] = {0.0};

        for (i = 0; i < size; ++i)
        {
            temp_str = vect_strs->at(i);
            start = end = 0;
            str_len = temp_str.size();

            for (j = 0; j < str_len; ++j)
            {
                k = 0;
                if (temp_str[j] == ' ' && k < 4)
                {
                    end = j;
                    env[k] = std::atof(temp_str.substr(start, end - start).c_str());
                    ++k;
                    start = j + 1;
                }
                if (4 <= k)
                    break;
            }

            vect_envs->push_back(new std::pair<geos::geom::Envelope *, int>(new geos::geom::Envelope(env[0], env[1], env[2], env[3]),
                                                                            std::atoi((temp_str.substr(start, str_len - start)).c_str())));
        }
    }

    void Reader ::Read_Geoms_mpi(std::string file_path, std::vector<std::string> *vect_strs, uint num_threads, MPI_Comm comm)
    {
        int num_world_nodes, my_world_rank;

        MPI_Comm_size(comm, &num_world_nodes);
        MPI_Comm_rank(comm, &my_world_rank);

        MPI_Info my_info;
        MPI_Info_create(&my_info);
        MPI_Info_set(my_info, "access_style", "read_once,sequential");
        MPI_Info_set(my_info, "collective_buffering", "true");
        MPI_Info_set(my_info, "romio_cb_read", "enable");

        MPI_Offset file_size;
        MPI_Offset local_size;
        MPI_Offset start;
        MPI_Offset end;

        MPI_File fh;

        MPI_File_open(comm, file_path.c_str(), MPI_MODE_RDONLY, my_info, &fh);

        MPI_File_get_size(fh, &file_size);

        local_size = file_size / num_world_nodes;
        start = my_world_rank * local_size;
        end = start + local_size - 1;

        //last process need to reach the end of file
        if (num_world_nodes - 1 == my_world_rank)
        {
            end = file_size - 1;
        }

        MPI_Offset chunk_size = end - start + 1;

        //Allocate memory for main chunk
        char *chunk;
        chunk = (char *)malloc((chunk_size + 1) * sizeof(char));

        assert(chunk);

        //To store incomplete strings
        char *front_chunk;
        char *back_chunk;

        MPI_File_read_at(fh, start, chunk, chunk_size, MPI_CHAR, MPI_STATUS_IGNORE);

        chunk[chunk_size] = '\0';

        //Get last string which should be incomplete
        MPI_Offset valid_end = chunk_size;

        if (num_world_nodes - 1 != my_world_rank)
        {
            for (; valid_end >= 0; --valid_end)
            {
                if (chunk[valid_end] == '\n')
                {
                    back_chunk = (char *)malloc((chunk_size - valid_end) * sizeof(char));
                    strncpy(back_chunk, chunk + valid_end + 1, chunk_size - valid_end);
                    break;
                }
            }
        }

        //Get first string which should be incomplete
        MPI_Offset valid_start = 0;
        if (0 != my_world_rank)
        {
            for (; valid_start < chunk_size; ++valid_start)
            {
                if (chunk[valid_start] == '\n')
                {
                    front_chunk = (char *)malloc((valid_start + 1) * sizeof(char));
                    strncpy(front_chunk, chunk, valid_start);
                    front_chunk[valid_start] = '\0';
                    ++valid_start;
                    break;
                }
            }
        }

        MPI_Offset str_start = valid_start;
        MPI_Offset str_end = valid_start;

        for (; valid_start <= valid_end; ++valid_start)
        {
            if (chunk[valid_start] == '\n' || valid_start == valid_end)
            {
                str_end = valid_start;
                MPI_Offset str_len = str_end - str_start;

                char *temp_chunk;

                temp_chunk = (char *)malloc((str_len + 1) * sizeof(char));
                strncpy(temp_chunk, chunk + str_start, str_len);

                temp_chunk[str_len] = '\0';

                vect_strs->push_back(temp_chunk);

                free(temp_chunk);

                str_start = valid_start + 1;
            }
        }

        char *recv_chunk;

        if (0 == my_world_rank % 2)
        {
            if (num_world_nodes - 1 != my_world_rank)
            {
                MPI_Offset send_len = strlen(back_chunk);

                MPI_Send(&send_len, 1, MPI_OFFSET, my_world_rank + 1, my_world_rank, MPI_COMM_WORLD);
                MPI_Send(back_chunk, send_len, MPI_CHAR, my_world_rank + 1, my_world_rank + 1, MPI_COMM_WORLD);
            }

            if (0 != my_world_rank)
            {
                MPI_Offset recv_len = 0;
                MPI_Status *status = new MPI_Status();

                MPI_Recv(&recv_len, 1, MPI_OFFSET, my_world_rank - 1, my_world_rank - 1, MPI_COMM_WORLD, status);
                recv_chunk = (char *)malloc((recv_len) * sizeof(char));
                MPI_Recv(recv_chunk, recv_len, MPI_CHAR, my_world_rank - 1, my_world_rank, MPI_COMM_WORLD, status);
            }
        }
        else
        {
            MPI_Offset recv_len = 0;
            MPI_Status *status = new MPI_Status();

            MPI_Recv(&recv_len, 1, MPI_OFFSET, my_world_rank - 1, my_world_rank - 1, MPI_COMM_WORLD, status);
            recv_chunk = (char *)malloc((recv_len) * sizeof(char));
            MPI_Recv(recv_chunk, recv_len, MPI_CHAR, my_world_rank - 1, my_world_rank, MPI_COMM_WORLD, status);

            if (num_world_nodes - 1 != my_world_rank)
            {
                MPI_Offset send_len = strlen(back_chunk);
                MPI_Send(&send_len, 1, MPI_OFFSET, my_world_rank + 1, my_world_rank, MPI_COMM_WORLD);
                MPI_Send(back_chunk, send_len, MPI_CHAR, my_world_rank + 1, my_world_rank + 1, MPI_COMM_WORLD);
            }
        }

        if (my_world_rank != 0)
        {
            std::string temp_str = std::string(recv_chunk) + std::string(front_chunk);
            vect_strs->push_back(temp_str);
        }

        MPI_File_close(&fh);
    }

    void Reader ::Reader_Load_part_file(std::string file_path, std::vector<std::string> *vect_strs, int divisor, MPI_Comm comm)
    {
        int num_world_nodes, my_world_rank;

        MPI_Comm_size(comm, &num_world_nodes);
        MPI_Comm_rank(comm, &my_world_rank);

        MPI_Info my_info;
        MPI_Info_create(&my_info);
        MPI_Info_set(my_info, "access_style", "read_once,sequential");
        MPI_Info_set(my_info, "collective_buffering", "true");
        MPI_Info_set(my_info, "romio_cb_read", "enable");

        MPI_Offset file_size;
        MPI_Offset local_size;
        MPI_Offset start;
        MPI_Offset end;

        MPI_Offset test_size = 0;
        MPI_Offset test_lines = 0;

        MPI_File fh;

        MPI_File_open(comm, file_path.c_str(), MPI_MODE_RDONLY, my_info, &fh);

        MPI_File_get_size(fh, &file_size);

        --file_size;

        local_size = file_size / divisor;
        start = (my_world_rank % divisor) * local_size;
        end = start + local_size - 1;

        printf("Size %lld : %lld, start %lld, end %lld, divisor %d \n",local_size, file_size, start, end,  divisor);
        //last process need to reach the end of file
        if ((divisor - 1) == (my_world_rank % divisor))
        {
            end = file_size - 1;
        }

        MPI_Offset chunk_size = end - start + 1;

        //Allocate memory for main chunk
        char *chunk;
        chunk = (char *)malloc((chunk_size + 1) * sizeof(char));

        assert(chunk);

        //To store incomplete strings
        char *front_chunk;
        char *back_chunk;

        MPI_File_read_at_all(fh, start, chunk, chunk_size, MPI_CHAR, MPI_STATUS_IGNORE);

        chunk[chunk_size] = '\0';

        //Get last string which should be incomplete
        MPI_Offset valid_end = chunk_size;

        if ((divisor - 1) != (my_world_rank % divisor))
        {
            for (; valid_end >= 0; --valid_end)
            {
                if (chunk[valid_end] == '\n')
                {
                    back_chunk = (char *)malloc((chunk_size - valid_end) * sizeof(char));
                    strncpy(back_chunk, chunk + valid_end + 1, chunk_size - valid_end);
                    break;
                }
            }
        }

        //Get first string which should be incomplete
        MPI_Offset valid_start = 0;
        if (0 != (my_world_rank % divisor))
        {
            for (; valid_start < chunk_size; ++valid_start)
            {
                if (chunk[valid_start] == '\n')
                {
                    front_chunk = (char *)malloc((valid_start + 1) * sizeof(char));
                    strncpy(front_chunk, chunk, valid_start);
                    front_chunk[valid_start] = '\0';
                    ++valid_start;
                    break;
                }
            }
        }

        MPI_Offset str_start = valid_start;
        MPI_Offset str_end = valid_start;

        for (; valid_start <= valid_end; ++valid_start)
        {
            if (chunk[valid_start] == '\n' || valid_start == valid_end)
            {
                
                str_end = valid_start;
                MPI_Offset str_len = str_end - str_start;

                test_size += str_len;
                test_lines++;
                char *temp_chunk;


                temp_chunk = (char *)malloc((str_len + 1) * sizeof(char));
                strncpy(temp_chunk, chunk + str_start, str_len);

                temp_chunk[str_len] = '\0';

                vect_strs->push_back(temp_chunk);

                free(temp_chunk);

                str_start = valid_start + 1;
            }
        }

        printf("Read Size %lld : %lld, lines %lld \n",test_size, local_size, test_lines);

        char *recv_chunk = NULL;

        // every process sends its back_chunk to its successor
        if ((divisor - 1) != (my_world_rank % divisor))
        {
            MPI_Request request;
            MPI_Offset send_len = strlen(back_chunk);

            MPI_Isend(&send_len, 1, MPI_OFFSET, my_world_rank + 1, my_world_rank, MPI_COMM_WORLD, &request);

            if (send_len > 0)
                MPI_Isend(back_chunk, send_len, MPI_CHAR, my_world_rank + 1, my_world_rank + 1, MPI_COMM_WORLD, &request);
        }

        // every process recv the back_chunk of its predecessor
        if (0 != (my_world_rank % divisor))
        {
            MPI_Offset recv_len = 0;
            MPI_Status status;

            MPI_Recv(&recv_len, 1, MPI_OFFSET, my_world_rank - 1, my_world_rank - 1, MPI_COMM_WORLD, &status);

            if (recv_len > 0)
            {
                recv_chunk = (char *)malloc((recv_len) * sizeof(char));
                MPI_Recv(recv_chunk, recv_len, MPI_CHAR, my_world_rank - 1, my_world_rank, MPI_COMM_WORLD, &status);
            }
        }

        if (0 != (my_world_rank % divisor))
        {
            std::string temp_str = std::string(recv_chunk) + std::string(front_chunk);
            vect_strs->push_back(temp_str);
        }
    }

} // namespace gsj

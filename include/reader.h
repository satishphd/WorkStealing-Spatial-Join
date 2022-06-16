#ifndef __GSJ_READER_H_INCLUDE__
#define __GSJ_READER_H_INCLUDE__

#include <global_var.h>

#include <vector>
#include <sstream>
#include <fstream>
#include <thread> // std::thread
#include <mutex>  // std::mutex
#include <list>
#include <stdlib.h>
#include <cstring>

#include <mpi.h>
#include <geos/geom/Geometry.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKBReader.h>

namespace gsj
{

    class Reader
    {
    public:
        void Read_Strs_from_file(const std::string file_path, std::vector<std::string> *vect_strs);

        void Read_Geoms_from_file(const std::string file_path, std::list<geos::geom::Geometry *> *list_geoms);

        void Read_Geoms_from_file_wkb(const std::string file_path, std::list<geos::geom::Geometry *> *list_geoms);

        void Read_Geoms_from_strs(std::vector<std::string> *vect_strs, std::list<geos::geom::Geometry *> *list_geoms);

        void Read_Geoms_from_strs_wkb(std::vector<std::string> *vect_strs, std::list<geos::geom::Geometry *> *list_geoms);

        void Read_Envs_from_strs(std::vector<std::string> *vect_strs,
                                 std::vector<geos::geom::Envelope *> *vect_envs);

        void Read_Envs_Weights_from_strs(std::vector<std::string> *vect_strs,
                                         std::vector<std::pair<geos::geom::Envelope *, int> *> *vect_envs);

        void Read_Geoms_from_strs_parallel(std::vector<std::string> *vect_strs, std::list<geos::geom::Geometry *> *list_geoms,
                                           uint num_threads = NUM_THREADS);

        void Read_Geoms_from_file_parallel(const std::string file_path, std::list<geos::geom::Geometry *> *list_geoms,
                                           uint num_threads = NUM_THREADS);

        void Read_Geoms_mpi(std::string file_path, std::vector<std::string> *vect_strs, uint num_threads,
                            MPI_Comm comm);

        void Util_Parse_string_parallel(std::vector<std::string> *vect_strs, std::list<geos::geom::Geometry *> *list_geoms, 
            uint num_threads);

        void Reader_Load_part_file(std::string file_path, std::vector<std::string> *vect_strs, int divisor, MPI_Comm comm);

        Reader(){};

        ~Reader(){};

    private:
        void thread_read_geoms_from_strs(std::vector<std::string> *vect_strs, std::list<geos::geom::Geometry *> *l_geoms,
                                    geos::io::WKTReader *reader, std::mutex *push_mutex);
    };

} // namespace gsj

#endif // ndef __GSJ_READER_H_INCLUDE__

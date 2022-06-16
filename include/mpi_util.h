#ifndef __GSJ_MPI_UTIL_H_INCLUDE__
#define __GSJ_MPI_UTIL_H_INCLUDE__

/* Global variables*/
#include <global_var.h>

/* c c++ headers */
#include <vector>
#include <list>
#include <map>
#include <queue>
#include <vector>
#include <stdlib.h>
#include <cstring>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <utility> // std::pair
#include <chrono>  // std::chrono::seconds, std::chrono::milliseconds
#include <mutex>   // std::mutex
#include <numa.h>  // numa
#include <numaif.h>
#include "sys/types.h" // For memory usage
#include "sys/sysinfo.h" // For memory usage

/* External libs */
#include <mpi.h>
#include <geos/index/strtree/STRtree.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Point.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/CoordinateArraySequenceFactory.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Polygon.h>
#include <spdlog/spdlog.h>
#include <spdlog/cfg/env.h> // support for loading levels from the environment variable
#include <wsq.hpp>

/* Internal libs */
#include <reader.h>
#include <wsq_manager.h>
#include <thread_util.h>
/* Functions */

namespace gsj
{
    void Global_Tasks_monitor_thread(WorkStealingQueue<std::pair<geos::geom::Geometry *,
                                                                 std::vector<geos::geom::Geometry *> *> *> **queue,
                                     int num_threads, MPI_Comm comm, volatile int * is_queue_filled, int num_nb_sendrecv, int num_tasks);

    void MPI_Util_send_buf_gen(std::list<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> *list_send_geoms,
                               ulong *send_buf, double *&send_buf_geoms, ulong *&send_buf_sizes, ulong *&send_map_arr);
                               
    void MPI_Util_parse_recv_buf(WorkStealingQueue<std::pair<geos::geom::Geometry *, std::vector<geos::geom::Geometry *> *> *> *queue,
                                 long unsigned int *recv_buf, double *recv_buf_geoms, long unsigned int *recv_buf_sizes, long unsigned int *recv_map_arr,
                                 std::mutex * thread_mutex);

    geos::geom::Geometry *arr_to_geoms(double *geomArray, ulong numVertices, const geos::geom::GeometryFactory *gf, ulong geom_type);
} //namespace gsj

#endif //ndef __GSJ_MPI_UTIL_H_INCLUDE__

#ifndef __GSJ_SPATIAL_JOIN_H_INCLUDE__
#define __GSJ_SPATIAL_JOIN_H_INCLUDE__

#include <global_var.h>

#include <list>
#include <vector>
#include <fstream>
#include <utility>      // std::pair
#include <stdlib.h>
#include <cstring>
#include <chrono>       // std::chrono::seconds, std::chrono::milliseconds

#include <geos/geom/Geometry.h>
#include <geos/geom/prep/PreparedGeometry.h>
#include <geos/geom/prep/PreparedGeometryFactory.h>
#include <geos/index/strtree/STRtree.h>
#include <geos/io/WKTReader.h>

#ifdef DEBUG
    #include <spdlog/spdlog.h>
    #include <spdlog/cfg/env.h> // support for loading levels from the environment variable
#endif //ifdef DEBUG

namespace gsj{

    std::list<geos::geom::Geometry*>* Spatial_Join_intersection(std::list<geos::geom::Geometry* > *l_geoms_1, 
            std::list<geos::geom::Geometry* > *l_geoms_2);

    std::list<std::pair<geos::geom::Geometry*, geos::geom::Geometry*>>* Spatial_Join_intersect(
            std::list<geos::geom::Geometry* > *l_geoms_1, std::list<geos::geom::Geometry* > *l_geoms_2);

} // namespace gsj

#endif // ndef __GSJ_SPATIAL_JOIN_H_INCLUDE__

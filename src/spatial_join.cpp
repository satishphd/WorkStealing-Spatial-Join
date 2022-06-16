#include <spatial_join.h>

namespace gsj{

std::list<geos::geom::Geometry*>* Spatial_Join_intersection(std::list<geos::geom::Geometry* > *l_geoms_1, 
        std::list<geos::geom::Geometry* > *l_geoms_2)
{
    #ifdef DEBUG
        auto t_index_begin = std::chrono::steady_clock::now();
    #endif //ifdef DEBUG

    std::list<geos::geom::Geometry*>* l_geoms_to_return = new std::list<geos::geom::Geometry*>();

    geos::index::strtree::STRtree index_for_layer_1;

    for(std::list<geos::geom::Geometry* >::iterator itr = l_geoms_1->begin(); itr != l_geoms_1->end(); ++itr)
    {
        geos::geom::Geometry* temp_geom =  *itr;
      
        index_for_layer_1.insert(temp_geom->getEnvelopeInternal(), temp_geom);
    }

    /************************Build index end************************************************************************/
    #ifdef DEBUG
        auto t_index_end = std::chrono::steady_clock::now();
    #endif //ifdef DEBUG

    /************************Spatial Join***************************************************************************/

    for(std::list<geos::geom::Geometry*>::iterator itr = l_geoms_2->begin(); itr != l_geoms_2->end(); ++itr )
    {
        geos::geom::Geometry* temp_geom = *itr;            
        std::vector<void *> results;

        const geos::geom::Envelope* temp_env=  temp_geom->getEnvelopeInternal();

        index_for_layer_1.query(temp_env, results);

        for(std::vector<void *>::iterator void_itr = results.begin(); void_itr != results.end(); void_itr ++)
        {
            void *temp_geom_ptr = *void_itr;
            geos::geom::Geometry* qrd_geom = (geos::geom::Geometry*)temp_geom_ptr;
            try{
                std::unique_ptr<geos::geom::Geometry> geoms_intersection = temp_geom->intersection(qrd_geom);
                l_geoms_to_return->push_back(geoms_intersection.release());
            }
            catch(std::exception &e)
            {
                //Do nothing, just skip the exception
            }
        }
    } 

    /************************Spatial Join end***********************************************************************/

    #ifdef DEBUG
        auto t_join_end = std::chrono::steady_clock::now();

        std::chrono::duration<double> t_build_index = t_index_end - t_index_begin;
        std::chrono::duration<double> t_spatial_join = t_join_end - t_index_end;
        spdlog::info("Spatial_Join_intersection ends: building index {0:03.3f}; spatial join {1:03.3f}",
                t_build_index.count(), t_spatial_join.count());
    #endif //ifdef DEBUG

    return l_geoms_to_return;
}

std::list<std::pair<geos::geom::Geometry*, geos::geom::Geometry*>>* Spatial_Join_intersect(
        std::list<geos::geom::Geometry* > *l_geoms_1, std::list<geos::geom::Geometry* > *l_geoms_2)
{
    #ifdef DEBUG
        auto t_index_begin = std::chrono::steady_clock::now();
    #endif //ifdef DEBUG

    std::list<std::pair<geos::geom::Geometry*, geos::geom::Geometry*>>* l_pairs_geoms_to_return = 
            new std::list<std::pair<geos::geom::Geometry*, geos::geom::Geometry*>>();

    geos::index::strtree::STRtree index_for_layer_1;

    for(std::list<geos::geom::Geometry* >::iterator itr = l_geoms_1->begin(); itr != l_geoms_1->end(); ++itr)
    {
        geos::geom::Geometry* temp_geom =  *itr;
      
        index_for_layer_1.insert(temp_geom->getEnvelopeInternal(), temp_geom);
    }

    /************************Build index end************************************************************************/
    #ifdef DEBUG
        auto t_index_end = std::chrono::steady_clock::now();
    #endif //ifdef DEBUG

    /************************Spatial Join***************************************************************************/

    for(std::list<geos::geom::Geometry*>::iterator itr = l_geoms_2->begin(); itr != l_geoms_2->end(); ++itr )
    {
        geos::geom::Geometry* temp_geom = *itr;            
        std::vector<void *> results;       
        std::unique_ptr<geos::geom::prep::PreparedGeometry> pg = 
                geos::geom::prep::PreparedGeometryFactory::prepare(temp_geom);

        const geos::geom::Envelope* temp_env=  temp_geom->getEnvelopeInternal();

        index_for_layer_1.query(temp_env, results);

        for(std::vector<void *>::iterator void_itr = results.begin(); void_itr != results.end(); void_itr ++)
        {
            void *temp_geom_ptr = *void_itr;
            geos::geom::Geometry* qrd_geom = (geos::geom::Geometry*)temp_geom_ptr;
            try{
                if(pg->intersects(qrd_geom))
                    l_pairs_geoms_to_return->push_back(std::make_pair(temp_geom, qrd_geom));
            }
            catch(std::exception &e)
            {
                //Do nothing, just skip the exception
            }
        }
    } 

    /************************Spatial Join end***********************************************************************/

    #ifdef DEBUG
        auto t_join_end = std::chrono::steady_clock::now();

        std::chrono::duration<double> t_build_index = t_index_end - t_index_begin;
        std::chrono::duration<double> t_spatial_join = t_join_end - t_index_end;
        spdlog::info("Spatial_Join_intersection ends: building index {0:03.3f}; spatial join {1:03.3f}",
                t_build_index.count(), t_spatial_join.count());
    #endif //ifdef DEBUG

    return l_pairs_geoms_to_return;
}

} // namespace gsj

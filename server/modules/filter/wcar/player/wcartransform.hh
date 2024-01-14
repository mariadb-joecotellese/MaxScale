#include "wcarplayerconfig.hh"

/** This class will massage the captured data to a form
 *  usable by the player.
 */
class Transform
{
public:
    Transform(const PlayerConfig* pConfig);
    Storage& player_storage();
private:
    const PlayerConfig&      m_config;
    std::unique_ptr<Storage> m_player_storage;
};

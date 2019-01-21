let express = require('express')
let router = express.Router()

router.get('/api.tiles/:z/:x/:y', (req, res) => {
    res.send(`You have requested a tile with a zoom level [${req.params.z}] and coordinates (${req.params.x}, ${req.params.y})`)
})


module.exports = router
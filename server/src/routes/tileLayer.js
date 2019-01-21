let express = require('express')
let router = express.Router()

const hbase = require('hbase')
const client = new hbase.Client({
    host: 'zappa',
    port: 8080
})

const tileRow = new hbase.Row(client, 'test', '1')

hbase()
.table('test')
.row('1')
.get('data:img', (error, value) => {
    console.info(value)
})


router.get('/api.tiles/:z/:x/:y', (req, res) => {
    res.send(`You have requested a tile with a zoom level [${req.params.z}] and coordinates (${req.params.x}, ${req.params.y})`)
})


module.exports = router
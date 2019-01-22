let express = require('express')
let router = express.Router()

const hbase = require('hbase')
/* 
const client = new hbase.Client({
    host: '0.0.0.0',
    port: 8080
})

const tileRow = new hbase.Row(client, 'test', '1') 
*/

let val

let options = {
    root: __dirname + '../../../public/img',
   // root: 'http://hbase.apache.org/images/',
    headers: {
        'x-timestamp': Date.now(),
        'x-sent': true
    }
}

const fileName = 'logo.png'

hbase()
.table('test')
.row('1')
.get('data:img', (error, value) => {
    val = value[0].$
    console.info(val)
})


router.get('/api.tiles/:z/:x/:y', (req, res, next) => {
    res.sendFile(fileName, options, (err) => {
        if (err) {
            next(err)
        } else {
            console.log('Sent: ', fileName)
        }
    })
    // res.send(`<h1>You have requested a tile with a zoom level [${req.params.z}] and coordinates (${req.params.x}, ${req.params.y}) \n <img src = "${val}"> </h1>`)
})

module.exports = router
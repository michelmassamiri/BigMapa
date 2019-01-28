let express = require('express')
let router = express.Router()

const hbase = require('hbase')

router.get('/api.tiles/:z/:x/:y', (req, res, next) => {
    if (req.params.x && req.params.y && req.params.z) {
        let rowID = req.params.x +','+ req.params.y
        console.log(rowID);
        
        let val
        hbase()
            .table('micheldomexic')
            .row(rowID)
            .get('data:image', (error, value) => {
                val = value[0].$
                let data = new Buffer(val, 'base64')
                console.log(val);
                res.contentType('image/png');
                res.send(data);
            })
    }
})

module.exports = router
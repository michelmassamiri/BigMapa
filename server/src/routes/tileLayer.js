let express = require('express')
let path = require('path')
let router = express.Router()

let app =  express()

const hbase = require('hbase')

app.use(express.static('public'))

router.get('/api.tiles/:z/:x/:y', (req, res, next) => {
    if (req.params.x && req.params.y && req.params.z) {
        let rowID = req.params.x +','+ (180-Number(req.params.y));
        console.log(rowID);
        
        let val
        hbase()
            .table('micheldomexic')
            .row(rowID)
            .get('data:image', (error, value) => {
                if (error) {
                    res.sendFile(path.join(__dirname, '../../public/default.png'))
                }
                else {
                    val = value[0].$
                    let data = new Buffer(val, 'base64')
                    res.contentType('image/png');
                    res.send(data);
                }
            })
    }
})

module.exports = router
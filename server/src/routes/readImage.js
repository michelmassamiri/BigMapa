let express = require('express');
let router = express.Router();
const hbase = require('hbase');
let fs = require("fs");

router.get('/api.tiles/readOutPut0', (req, res, next) => {
    fs.readFile('output0.png', function(err, data) {
        if (err) {
            return res.status(500).send({error:{status: 500}, message: 'Invalid request'});
        }

        // Encode to base64
        let encodedImage = new Buffer(data, 'binary');
        res.contentType('image/png');
        res.send(encodedImage);
        // Decode from base64
        //let decodedImage = new Buffer(encodedImage, 'base64');
    });
});

router.get('/api.tiles/:z/:x/:y', function (req, res, next) {
    if(!req.params.x || !req.params.y) {
        return res.status(500).send({error:{status: 500}, message: 'Invalid request'});
    }

    let rowId = req.params.x + ',' + req.params.y;
    hbase()
        .table('michelmassamiri')
        .row(rowId)
        .get('data:image', (error, value) => {
            if(error){
                /* return water */
                return res.status(404).send({error: {status: 404, message: 'NOT FOUND'}});
            }

            let val = value[0].$;
            console.info(val);
            let encodedImage = new Buffer(val, 'binary');
            res.contentType('image/png');
            res.send(encodedImage);
        });
});

module.exports = router;
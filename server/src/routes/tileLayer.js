let express = require('express')
let fs = require('fs')
let router = express.Router()

const hbase = require('hbase')
/* 
const client = new hbase.Client({
    host: '0.0.0.0',
    port: 8080
})

const tileRow = new hbase.Row(client, 'test', '1') 
*/


let options = {
    root: __dirname + '../../../public/',
    headers: {
        'x-timestamp': Date.now(),
        'x-sent': true
    }
}

const fileName = 'public/img.png'




router.get('/api.tiles/:x/:y/:z', (req, res, next) => {
    if (req.params.x && req.params.y && req.params.z) {
        let rowID = req.params.x +','+ req.params.y
        console.log(rowID);
        
        let val
        hbase()
            .table('michelmassamiri')
            .row(rowID)
            .get('data:image', (error, value) => {
                val = value[0].$
                let data = hexa2img(val)
                fs.writeFile(fileName, data, (err) => {
                    if (err) throw err;
                    console.log('The file has been saved!');
                    res.sendFile(`img.png`, options, (err2) => {
                        if (err2) {
                            next(err2)
                        } else {
                            console.log('Sent: ', fileName)
                        }
                    })
                    res.on('finish', function () {
                        // Check if the file exists in the current directory.
                        if (!fs.existsSync(fileName)) {
                            console.log(fileName, 'doesn\'t exist')
                        } 
                        else {
                            // Have to remove setTimout !!!!
                            setTimeout(function () {
                                try {
                                    fs.unlinkSync(fileName);
                                    console.log(fileName, 'was deleted');
                                } catch (e) {
                                    console.log("error removing ", fileName);
                                }
                            }, 000)
                        }
                    }); //res.on(...)
                }); // fs.writeFile(...) 
            }) // .get(...)
    }
})

function clean_hex(rowData) {
    rowData = rowData.toUpperCase();
    // Perform a (g) global, (i) case-insensitive replacement
    rowData = rowData.replace(/0x/gi, "");
    // Perform a (g) global replacement
    rowData = rowData.replace(/[^A-Fa-f0-9]/g, "");
    
    return rowData;
}

function hexa2img(hexa) {
    var cleaned_hex = clean_hex(hexa);

    // if (cleaned_hex.length % 2) {
       // alert("Error: cleaned hex string length is odd.");
       // return;
    // }

    var binary = new Array();
    for (var i = 0; i < cleaned_hex.length / 2; i++) {
        var h = cleaned_hex.substr(i * 2, 2);
        binary[i] = parseInt(h, 24);
    }

    var byteArray = new Uint8Array(binary);
    return byteArray
}

module.exports = router
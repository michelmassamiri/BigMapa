let express = require('express')
let app = express()

let tileLayerRoute = require('./routes/tileLayer')

app.use(tileLayerRoute)

const PORT = process.env.PORT || 3000
app.listen(PORT, () => console.info(`Server listen on port ${PORT}...`))
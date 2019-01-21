let express = require('express')
let app = express()

let tileLayerRoute = require('./routes/tileLayer')

app.use(tileLayerRoute)

app.use((req, res, next) => {
    res.status(404).send('Error 404: We can not find the requested resource !')
})

const PORT = process.env.PORT || 3000
app.listen(PORT, () => console.info(`Server listen on port ${PORT}...`))
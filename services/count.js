const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = (client, from, to, callback) => {
    client
    .count({
        index: indexName,
        body: {
            query : {
                range : {
                    "@timestamp": {
                        lt: to, 
                        gte: from
                    }
                }
            }
        }
    })
    .then(resp => {
        callback({
            count: resp.body.count
        })
    });

    
}

exports.countAround = (client, lat, lon, radius, callback) => {
    
    client
    .count({
        index: indexName,
        body: {
            query : {
                geo_distance : {
                    distance : radius,
                    location : {
                        lat : lat,
                        lon : lon
                    }
                }
            }
        }
    })
    .then(resp => {
        callback({
            count: resp.body.count
        })
    });
}
const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    client
    .search({
        index: indexName,
        size: 0,
        body: {
            aggs : {
                arrondissement : {
                    terms: {
                        field: "arrondissement.keyword",
                        size: 1000000
                    }
                }
            }
        }
    })
    .then(resp => {

        let response = resp.body.aggregations.arrondissement.buckets.map(res => {
            return {
                "arrondissement": res.key,
                "count": res.doc_count
            }
        });

        Promise.all(response).then(callback(response));
        
    });
}

exports.statsByType = (client, callback) => {
    client
    .search({
        index: indexName,
        size: 0,
        body: {
            aggs: {
                types : {
                    terms: {
                        field: "type.keyword",
                        size: 5
                    },
                    aggs : {
                        count : {
                            terms : {
                                field : "sous_type.keyword",
                                size: 5
                            }
                        }
                    }
                }
              }
        }
    })
    .then(resp => {
        let response = resp.body.aggregations.types.buckets.map(res => {
            let retObj = {
                "type": res.key,
                "count": res.doc_count,
                "sous_types": ""
            }

            retObj.sous_types = res.count.buckets.map(countObj => {
                return {
                    "sous_type": countObj.key,
                    "count": countObj.doc_count
                }
            });

            return retObj;
        });

        Promise.all(response).then(callback(response));
        
    });
}

exports.statsByMonth = (client, callback) => {
    client
    .search({
        index: indexName,
        size: 0,
        body: {
            aggs : {
                mois : {
                  date_histogram: {
                      field: "@timestamp",
                      calendar_interval : "month",
                      order: {
                         "_count": "desc"
                      }
                  }
                }
            }
        }
    })
    .then(resp => {
        let response =  resp.body.aggregations.mois.buckets.slice(0, 10).map(res => {
            let date = new Date(res.key_as_string);
            let month = (date.getMonth() + 1).toString().padStart(2, "0");
            return {
                "month": month + "/" + date.getFullYear(),
                "count": res.doc_count
            }
        });

        Promise.all(response).then(callback(response));
    });
}

exports.statsPropreteByArrondissement = (client, callback) => {
    console.log("Before starting...");
    client
    .search({
        index: indexName,
        size: 0,
        body: {
            aggs : {
                arrondissement_agg : {
                    filter : {
                        term: { 
                            type: "propreté" 
                        } 
                    },
                    aggs : {
                        arrondissement : {
                            terms: {
                                field: "arrondissement.keyword",
                                size : 3
                            }
                        }
                    }	
                }
            }
        }
    })
    .then(resp => {
        let response = resp.body.aggregations.arrondissement_agg.arrondissement.buckets.map(res => {
            return {
                "arrondissement": res.key,
                "count": res.doc_count
            }
        });

        Promise.all(response).then(callback(response));
    });
}

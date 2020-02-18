const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run () {
    // Create Elasticsearch client
    const client = new Client({ 
        node: config.get('elasticsearch.uri'), 
        requestTimeout: 600000 
    });

    // Creer mapping
    client.indices.create({
        index: indexName,
        body: {
            "mappings": {
                "properties": {
                    "location": {
                        "type": "geo_point"
                    }
                }
            }
        }
    }, function (err, resp, respcode) {
        console.log("Response", resp);
    });
  
   
    // Read CSV file
    let line_index = 0;
    let anomalies=[]
    let promises = [];


    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {

            // Increment sub array index
            if(line_index%10000===0 && line_index!=0){
                // Copy array
                const anomaliesDuplicate = [...anomalies];
                
                // Empty array
                anomalies.length = 0;

                promises.push(new Promise((resolve, reject) => {
                    client.bulk(createBulkInsertQuery(anomaliesDuplicate), (err, resp) => {
                        if (err){
                            console.trace(err.message);
                            reject();
                        }
                            
                        else {
                            console.log(`Inserted ${resp.body.items.length} anomalies`);
                            resolve();
                        }
                    })
                }));
            }

            anomalies.push({
                '@timestamp' : data["DATEDECL"],
                object_id : data["OBJECTID"],
                annee_declaration : data["ANNEE DECLARATION"],
                mois_declaration : data["MOIS DECLARATION"],
                type : data["TYPE"],
                sous_type : data["SOUSTYPE"],
                code_postal : data["CODE_POSTAL"],
                ville : data["VILLE"],
                arrondissement : data["ARRONDISSEMENT"],
                prefixe : data["PREFIXE"],
                intervenant : data["INTERVENANT"],
                conseil_de_quartier : data["CONSEIL DE QUARTIER"],
                location : data["geo_point_2d"]
            });            

            // Increment index
            line_index++;
        })
        .on('end', () => {
            // Make last insert
            Promise.all(promises).then(() => {
                client.bulk(createBulkInsertQuery(anomalies), (err, resp) => {
                    if (err)
                        console.trace(err.message);
                    else
                        console.log(`Inserted ${resp.body.items.length} anomalies`);

                    console.log("Closing connection");
                    client.close();
                })
            });
        });

        function createBulkInsertQuery(anoms) {
            const body = anoms.reduce((an, anom) => {
              an.push({ index: { _index: indexName, _type: '_doc', _id: anom.object_id } })
              an.push(anom)
              return an
            }, []);
            
            return { 
                body: body,
                refresh: "wait_for"
            };
        }
}

run().catch(console.error);
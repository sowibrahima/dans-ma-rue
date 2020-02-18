const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run () {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    // TODO il y a peut être des choses à faire ici avant de commencer ... 
    // Création de l'indice
    client.indices.create({ index: indexName }, (err, resp) => {
        if (err) console.trace(err.message);
    });

  
    for (var anomalies=[]; anomalies.push([])<172;);


    // Read CSV file
    let line_index = 0;
    let array_index = 0;


    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {

            // Increment sub array index
            if(line_index%5000 === 0 && line_index!=0){
                array_index++;
                console.log("5k lines inserted, current index is", line_index);
            }

            anomalies[array_index].push({
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
            // TODO il y a peut être des choses à faire à la fin aussi ?
            for(let anomalyArray of anomalies){
                client.bulk(createBulkInsertQuery(anomalyArray), (err, resp) => {
                    if (err) 
                        console.trace(err.message);
                    else 
                        console.log(`Inserted ${resp.body.items.length} anomalies`);
                  });
            }
            //console.log('Terminated!');
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


[
    [], // 50k
    [], // 50k
    [] // 50k
]
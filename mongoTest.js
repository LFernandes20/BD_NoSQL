use iberotrem;

//show collections

//db.Viagem.find().pretty()

//db.Cliente.find().pretty()

//db.Reserva.find().pretty()

// Reservas feitas em 2017
db.Reserva.find({Data.getYear(): "2017"});


// Nome dos clientes com Reservas cuja viagem tenha destino na Estação de Campanhã

function checkReserva (estacao) {
    var destino = db.Viagem.find({"EstaçãoDestino.Nome": estacao});
    var documentD = destino.hasNext() ? destino.next() : null;
    if (documentD) {
        var viagemId = documentD._id;
        var reserva = db.Reserva.find({"Viagem_id": viagemId})
        var documentR = reserva.hasNext() ? reserva.next() : null;
        if (documentR) {
            var clientId = documentR.Cliente_id
            return db.Cliente.find({"_id": clientId}, {Nome: 1, _id: 0})
        }
    }
}

// checkReserva("Campanhã");



// Transação Inserir Cliente

function insertCliente(nome, cc, dataNascimento, tele, email) {
    var dN = new ISODate(dataNascimento);
    db.Cliente.insert({"_id": cc,
                       "Data_de_Nascimento": dN,
                       "Nome": nome,
                       "Telefone": tele,
                       "Email": email});
}

//insertCliente("Tony Sala Aberta", "76649845", "2017-01-16", "913457447", "tonysalinha@di.uminho.pt");

//db.Cliente.find().pretty();


// Transação Inserir Viagem

function insertViagem (horaP, horaC, preco, nomeEO, cidadeEO, nomeED, cidadeED, nrL) {
    var ultViagem = db.Viagem.find().sort({_id:-1}).limit(1);
    var documentV = ultViagem.hasNext() ? ultViagem.next() : null;
    if (documentV) {
        var viagemIdAnt = documentV._id;
        var valor = viagemIdAnt.substring(6);
        valor++;
        var viagemIdNew = "viagem"+valor;
        var i = 1;
        var lugar = [];
        while (i <= nrL) {
            lugar.push({"Nr": i});
            i++;
        }
        db.Viagem.insert({"_id": viagemIdNew,
                          "Hora_partida": horaP,
                          "Hora_chegada": horaC,
                          "Preço": preco,
                          "EstaçãoOrigem": {"Nome": nomeEO,
                                            "Cidade": cidadeEO},
                          "EstaçãoDestino": {"Nome": nomeED,
                                             "Cidade": cidadeED},
                          "Comboio": {"Nr_lugares": nrL,
                                      "Lugar": lugar}});
    }
}

//insertViagem ("03:06:00", "09:10:00", 60.0, "Oriente", "Lisboa", "Campanhã", "Porto", 16);

//db.Viagem.find().sort({_id: -1}).limit(1).pretty();




// Transação Inserir Reserva

function insertReserva(lugar, data, cli, eO, eD) {
    var viagem = db.Viagem.find({$and:[{'EstaçãoOrigem.Nome': eO}, {'EstaçãoDestino.Nome': eD}]}) 
    var documentD = viagem.hasNext() ? viagem.next() : null;
    if (documentD) {
        var viagemId = documentD._id;
        var precoViagem = documentD.Preço;
        var reserva = db.Reserva.find().sort({_id:-1}).limit(1);
        var documentR = reserva.hasNext() ? reserva.next() : null;
        if (documentR) {
            var reservaIdAnt = documentR._id;
            var valor = reservaIdAnt.substring(7);
            valor++;
            var reservaIdNew = "reserva"+valor;
            var cliente = db.Cliente.find({"_id": cli}); 
            var documentC = cliente.hasNext() ? cliente.next() : null;
            if (documentC) {
                var clienteDN = documentC.Data_de_Nascimento;
                var today = new Date();
                var dd = today.getDate();
                var mm = today.getMonth() + 1;
                var yyyy = today.getFullYear();
                if (dd < 10) {
                    dd = "0" + dd;
                } 
                if (mm < 10) {
                    mm = "0" + mm;
                } 
                today = yyyy + "-" + mm + "-" + dd;
                var current = new ISODate (today);
                var date = new ISODate(data);                        
                var yearMS = 365 * 24 * 60 * 60 * 1000;
                var idade = parseFloat((current - clienteDN)/yearMS).toFixed(2);
                if (idade > 25) {
                    db.Reserva.insert({"_id": reservaIdNew,
                                       "Lugar": lugar,       
                                       "Data": date,
                                       "Cliente_id": cli,
                                       "Viagem_id": viagemId,
                                       "Preço": precoViagem});
                }
                else {
                    var precoFinal = (1 - 0.25) * precoViagem;
                    db.Reserva.insert({"_id": reservaIdNew,
                                       "Lugar": lugar,  
                                       "Data": date,
                                       "Cliente_id": cli,   
                                       "Viagem_id": viagemId,
                                       "Desconto": "25%",
                                       "Preço": precoFinal});
                }
            }
        }
    }
}

//insertReserva(4, "2017-01-25", "54365476", "Campanhã", "Vigo Guixar");

//db.Reserva.find().pretty();



// Define the callback function - forEach

function addMatch(value, index, ar) {
    cmd.push("{ $match: {'Comboio.Lugar.Nr': {$ne: " + value + "} } }, ");
}

// Apresentar lugares livres numa dada data para uma viagem cuja viagem tenha origem na estação X e destino na estação Y

function lugaresLivres(data, eO, eD){
  var viagem = db.Viagem.find({$and:[{'EstaçãoOrigem.Nome': eO}, {'EstaçãoDestino.Nome': eD}]}); 
  var documentV = viagem.hasNext() ? viagem.next() : null;
  if (documentV) {
    var idViagem = documentV._id;
    var date = new ISODate(data);
    var reserva = db.Reserva.find({$and: [{Viagem_id: idViagem}, {Data: date} ] });
    var ocupados = [];
    while (reserva.hasNext()) {
        var documentR = reserva.next();
        ocupados.push(documentR.Lugar);
    }
    var cmd = [];
    cmd.push("db.Viagem.aggregate([ {$unwind: '$Comboio.Lugar'}, {$match: {_id: '");
    cmd.push(idViagem, "' } }, ");
    var i;
    for (i = 0; i < ocupados.length; i++){
      cmd.push("{ $match: {'Comboio.Lugar.Nr': {$ne: " + ocupados[i] + "} } }, ");
    }
    cmd.push("{$project: { _id: 0, Hora_partida: 0, Hora_chegada: 0, Preço: 0, EstaçãoOrigem: 0, EstaçãoDestino: 0, 'Comboio.Nr_lugares': 0 } } ] )");
    return cmd.join("");
  }
}

//db.Viagem.aggregate([ {$unwind: '$Comboio.Lugar'}, {$match: {_id: 'viagem1' } }, { $match: {'Comboio.Lugar.Nr': {$ne: 9} } }, { $match: {'Comboio.Lugar.Nr': {$ne: 1} } }, { $match: {'Comboio.Lugar.Nr': {$ne: 4} } }, {$project: { _id: 0, Hora_partida: 0, Hora_chegada: 0, Preço: 0, EstaçãoOrigem: 0, EstaçãoDestino: 0, 'Comboio.Nr_lugares': 0 } } ] )
var reservas = eval(lugaresLivres("2017-01-25", "Campanhã", "Vigo Guixar"));
var nr = 0;
while(reservas.hasNext()) {
	nr++;
	reservas.next();
};
nr;

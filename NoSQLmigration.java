/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nosqlmigration;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import static java.lang.Integer.parseInt;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import org.bson.Document;

public class NoSQLmigration {

    //Estabelecer a conexão à BD relacional
    private static final String USERNAME = "root";
    private static final String PASSWORD = "****";
    private static final String URL = "127.0.0.1:3306";
    private static final String SCHEMA = "iberotrem";
    

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");   
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://"+URL+"/"+SCHEMA+"?user="+USERNAME+"&password="+PASSWORD);
    }
    
    //Estabelecer a ligação à BD de Mongo a ser criada
    static final Mongo mongoClient = new MongoClient();
    static final DB iberotrem = mongoClient.getDB("iberotrem");
    static DBCollection cliente = iberotrem.getCollection("Cliente");
    //static DBCollection estacao = iberotrem.getCollection("Estação");
    //static DBCollection comboio = iberotrem.getCollection("Comboio");
    static DBCollection viagem = iberotrem.getCollection("Viagem");
    static DBCollection reserva = iberotrem.getCollection("Reserva");
    
//Cria Bd e coleções
    public static void collectClientes(){
        try {
            Connection c = connect();
            ResultSet rs = c.createStatement().executeQuery("SELECT * FROM Cliente");
            while (rs.next()) {
                String cc = rs.getString("CC");
                Date data = rs.getDate("Data_de_Nascimento");
                String nome = rs.getString("Nome");
                String tel = rs.getString("Telefone");
                String email = rs.getString("Email");
            
                BasicDBObject doc_cliente = new BasicDBObject();
                doc_cliente.put("_id", cc);
                doc_cliente.put("Data_de_Nascimento", data);
                doc_cliente.put("Nome", nome);
                doc_cliente.put("Telefone", tel);
                doc_cliente.put("Email", email);
                cliente.insert(doc_cliente);
            }
        }
        catch (SQLException | NullPointerException e){
            System.out.println(e.getMessage());
        }
    }
    
    /*
    public static void collectEstacoes(){
        try {
            Connection c = connect();
            ResultSet rs = c.createStatement().executeQuery("SELECT * FROM Estação");
            int i = 1;
            while (rs.next()) {
                String nome = rs.getString("Nome");
                String cidade = rs.getString("Cidade");
            
                BasicDBObject doc_estacao = new BasicDBObject();
                doc_estacao.put("_id", "estacao"+i);
                doc_estacao.put("Nome", nome);
                doc_estacao.put("Cidade", cidade);
                estacao.insert(doc_estacao);
                i++;
            }
        }
        catch (SQLException | NullPointerException e){
            System.out.println(e.getMessage());
        }
    }
    */
    
    /*  
    public static void collectComboios(){
        try {
            Connection c = connect();
            ResultSet rs = c.createStatement().executeQuery("SELECT * FROM Comboio");
            int j = 1;
            while (rs.next()) {
                int lugares = rs.getInt("Nr_lugares");
            
                BasicDBObject doc_comboio = new BasicDBObject();
                doc_comboio.put("_id", "comboio"+j);
                j++;
                doc_comboio.put("Nr_lugares", lugares);
                BasicDBObject[] lugaresC = new BasicDBObject[lugares];
                int i = 0;
                for (BasicDBObject obj : lugaresC) {
                    BasicDBObject lugar = new BasicDBObject();
                    lugar.put("Nr", i);
                    lugaresC[i] = lugar;
                    i++;
                }
                doc_comboio.put("Lugar", lugaresC);
                comboio.insert(doc_comboio);
            }
        }
        catch (SQLException | NullPointerException e){
            System.out.println(e.getMessage());
        }
    }
    */
    
    
    public static void collectViagens(){
        try {
            Connection c = connect();
            ResultSet rs = c.createStatement().executeQuery("SELECT * FROM Viagem");
            int i = 1;
            while (rs.next()) {
                String horaP = rs.getString("Hora_Partida");
                String horaC = rs.getString("Hora_Chegada");
                float preco = rs.getFloat("Preço");
                int estacaoO = rs.getInt("Id_estação_origem");
                int estacaoD = rs.getInt("Id_estação_destino");
                int comb = rs.getInt("Id_comboio");
            
                BasicDBObject doc_viagem = new BasicDBObject();
                doc_viagem.put("_id", "viagem"+i);
                i++;
            
                doc_viagem.put("Hora_partida", horaP);
                doc_viagem.put("Hora_chegada", horaC);
            
                DecimalFormat df = new DecimalFormat("###.##");
                df.format(preco);
                doc_viagem.put("Preço", preco);
                
                ResultSet rsEO = c.createStatement().executeQuery("SELECT * FROM Estação WHERE Id_estação="+estacaoO);
                while (rsEO.next()) {
                    String nomeEO = rsEO.getString("Nome");
                    String cidadeEO = rsEO.getString("Cidade");
                    BasicDBObject doc_estacaoO = new BasicDBObject();
                    doc_estacaoO.put("Nome", nomeEO);
                    doc_estacaoO.put("Cidade", cidadeEO);
                    doc_viagem.put("EstaçãoOrigem", doc_estacaoO);
                }
                
                ResultSet rsED = c.createStatement().executeQuery("SELECT * FROM Estação WHERE Id_estação="+estacaoD);
                while (rsED.next()) {
                    String nomeED = rsED.getString("Nome");
                    String cidadeED = rsED.getString("Cidade");
                    BasicDBObject doc_estacaoD = new BasicDBObject();
                    doc_estacaoD.put("Nome", nomeED);
                    doc_estacaoD.put("Cidade", cidadeED);
                    doc_viagem.put("EstaçãoDestino", doc_estacaoD);
                }
                
                ResultSet rsC = c.createStatement().executeQuery("SELECT * FROM Comboio WHERE Id_comboio="+comb);
                while (rsC.next()) {
                    int nrL = rsC.getInt("Nr_lugares");
                    BasicDBObject doc_comb = new BasicDBObject();
                    doc_comb.put("Nr_lugares", nrL);
                    
                    BasicDBObject[] lugaresC = new BasicDBObject[nrL];
                    int j = 1;
                    for (BasicDBObject obj : lugaresC) {
                        BasicDBObject lugar = new BasicDBObject();
                        lugar.put("Nr", j);
                        lugaresC[j-1] = lugar;
                        j++;
                    }
                    doc_comb.put("Lugar", lugaresC);
                    
                    doc_viagem.put("Comboio", doc_comb);
                }
            
            viagem.insert(doc_viagem);
            }
        } catch (SQLException | NullPointerException e){
            System.out.println(e.getMessage());
        }
    }
    
    
    public static int ageCalculator(String[] dob){
        Calendar now = new GregorianCalendar();
        int age = now.get(Calendar.YEAR) - Integer.parseInt(dob[0]) - 1;
        //System.out.println(age);
        if (now.get(Calendar.MONTH)+1 > Integer.parseInt(dob[1]))
            age++;
        else if (now.get(Calendar.MONTH) == Integer.parseInt(dob[1]))
                if (now.get(Calendar.DAY_OF_MONTH) >= Integer.parseInt(dob[2]))
                    age++;
        return age;
    }
    
    public static void collectReservas(){
        try {
            Connection c = connect();
            ResultSet rs = c.createStatement().executeQuery("SELECT * FROM Reserva");
            int i = 1;
            while (rs.next()) {
                int lugar = rs.getInt("Lugar");
                Date data = rs.getDate("Data");
                String cc = rs.getString("CC");
                int viagemId = rs.getInt("Id_viagem");
            
                BasicDBObject doc_reserva = new BasicDBObject();
                doc_reserva.put("_id", "reserva"+i);
                i++;
                doc_reserva.put("Lugar", lugar);
                doc_reserva.put("Data", data);
                doc_reserva.put("Cliente_id", cc);
                doc_reserva.put("Viagem_id", "viagem"+viagemId);
            
                double precoViagem = 0;
                double preco = 0;
                String dob = "";
                ResultSet rss = c.createStatement().executeQuery("SELECT Preço FROM Viagem WHERE Id_viagem="+viagemId);
                if (rss.next()) {
                    precoViagem = rss.getFloat("Preço");
                }
                ResultSet rsss = c.createStatement().executeQuery("SELECT Data_de_Nascimento FROM Cliente WHERE CC=\""+cc+"\"");
                if (rsss.next()) {
                    dob = rsss.getString("Data_de_Nascimento");
                }
                String[] sss = dob.split("-");
                if (ageCalculator(sss) < 25) {
                    double desc = 0.25;
                    doc_reserva.put("Desconto", "25%");
                    preco = precoViagem * (1 - desc);
                    DecimalFormat df = new DecimalFormat("###.##");
                    doc_reserva.put("Preço", df.format(preco));
                }
                else {
                    preco = precoViagem;
                    DecimalFormat df = new DecimalFormat("###.##");
                    doc_reserva.put("Preço", df.format(preco));
                }
                reserva.insert(doc_reserva);
            }
        }
        catch (SQLException | NullPointerException e){
            System.out.println(e.getMessage());
        }
    }
    
    
    public static void main(String[] args) throws SQLException{
        collectClientes();
        //collectEstacoes();
        //collectComboios();
        collectViagens();
        collectReservas();
    }
    
}

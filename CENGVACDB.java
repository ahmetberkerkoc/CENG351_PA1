package ceng.ceng351.cengvacdb;

import ceng.ceng351.cengvacdb.*;

import java.sql.*;
import java.util.ArrayList;

public class CENGVACDB implements ICENGVACDB {

    private static String user = "e2232320"; // TODO: Your userName
    private static String password = "5TsW8LyxHklD"; //  TODO: Your password
    private static String host = "144.122.71.121"; // host name
    private static String database = "db2232320"; // TODO: Your database name
    private static int port = 8080; // port

    private static Connection connection = null;

    @Override
    public void initialize() {
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection =  DriverManager.getConnection(url, user, password);
        }
        catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int createTables() {
        int count_created_table =0;
        String createUserTable = "create table if not exists user(" +
                "userID int not null," +
                "userName varchar(30)," +
                "age int," +
                "address varchar(150)," +
                "password varchar(30)," +
                "status varchar(15)," +
                "primary key (userID));";
        String createVaccineTable = "create table if not exists vaccine (" +
                "code int not null," +
                "vaccine_name varchar(30)," +
                "vaccine_type varchar(30)," +
                "primary key(code));";
        String createVaccinationTable = "create table if not exists vaccination (" +
                "code int not null," +
                "userID int not null," +
                "dose int not null," +
                "vac_date date," +
                "primary key (code,userID,dose)," +
                "foreign key (code) references vaccine(code) "+
                "on delete cascade," +
                "foreign key (userID) references user(userID));";

        String createAllergicSideEffectTable = "create table if not exists AllergicSideEffect (" +
                "effect_code int not null," +
                "effect_name varchar(50)," +
                "primary key (effect_code));";
        String createSeenTable = "create table if not exists seen (" +
                "effect_code int not null," +
                "code int not null," +
                "userID int not null," +
                "seen_date date," +
                "seen_degree varchar(30)," +
                "primary key (effect_code,code,userID)," +
                "foreign key (effect_code) references AllergicSideEffect(effect_code),"+
                "foreign key (code) references vaccination(code) on delete cascade,"+
                "foreign key (userID) references user(userID));";

        try {
           Statement statement = connection.createStatement();

           statement.executeUpdate(createUserTable);
           count_created_table++;

           statement.executeUpdate(createVaccineTable);
           count_created_table++;

           statement.executeUpdate(createVaccinationTable);
           count_created_table++;

           statement.executeUpdate(createAllergicSideEffectTable);
           count_created_table++;

           statement.executeUpdate(createSeenTable);
           count_created_table++;

           statement.close();
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return count_created_table;
    }

    @Override
    public int dropTables() {
        int count_dropped_table=0;

        String dropUser = "drop table if exists user";
        String dropVaccine = "drop table if exists vaccine";
        String dropVaccination = "drop table if exists vaccination";
        String dropAllergicSideEffect = "drop table if exists AllergicSideEffect";
        String dropSeen = "drop table if exists seen";

        try {
            Statement statement = connection.createStatement();

            statement.executeUpdate(dropSeen);
            count_dropped_table++;

            statement.executeUpdate(dropAllergicSideEffect);
            count_dropped_table++;

            statement.executeUpdate(dropVaccination);
            count_dropped_table++;

            statement.executeUpdate(dropVaccine);
            count_dropped_table++;

            statement.executeUpdate(dropUser);
            count_dropped_table++;




            statement.close();
        }
        catch (SQLException e){
            e.printStackTrace();
        }


        return count_dropped_table;
    }

    @Override
    public int insertUser(User[] users) {
        int count_instered_user=0;

        for(int i=0;i<users.length;i++){
            String user_query = "insert into user values (\"" +
                    users[i].getUserID() + "\",\"" +
                    users[i].getUserName() + "\",\"" +
                    users[i].getAge() + "\",\"" +
                    users[i].getAddress() + "\",\"" +
                    users[i].getPassword() + "\",\"" +
                    users[i].getStatus() + "\")";

        try {
            Statement statement = connection.createStatement();

            statement.executeUpdate(user_query);
            count_instered_user++;

            statement.close();
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        }


        return count_instered_user;
    }

    @Override
    public int insertAllergicSideEffect(AllergicSideEffect[] sideEffects) {
        int count_instered_AllergicSideEffect=0;

        for(int i=0;i<sideEffects.length;i++){
            String AllergicSideEffect_query = "insert into AllergicSideEffect values (\"" +
                    sideEffects[i].getEffectCode() + "\",\"" +
                    sideEffects[i].getEffectName() +"\")";


            try {
                Statement statement = connection.createStatement();

                statement.executeUpdate(AllergicSideEffect_query);
                count_instered_AllergicSideEffect++;

                statement.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }


        return count_instered_AllergicSideEffect;
    }

    @Override
    public int insertVaccine(Vaccine[] vaccines) {
        int count_instered_vaccine=0;

        for(int i=0;i<vaccines.length;i++){
            String vaccine_query = "insert into vaccine values (\"" +
                    vaccines[i].getCode() + "\",\"" +
                    vaccines[i].getVaccineName() + "\",\"" +
                    vaccines[i].getType() +"\")";


            try {
                Statement statement = connection.createStatement();

                statement.executeUpdate(vaccine_query);
                count_instered_vaccine++;

                statement.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }


        return count_instered_vaccine;
    }

    @Override
    public int insertVaccination(Vaccination[] vaccinations) {
        int count_instered_vaccinations=0;

        for(int i=0;i<vaccinations.length;i++){
            String vaccination_query = "insert into vaccination values (\"" +
                    vaccinations[i].getCode() + "\",\"" +
                    vaccinations[i].getUserID() + "\",\"" +
                    vaccinations[i].getDose() + "\",\"" +
                    vaccinations[i].getVacdate() +"\")";


            try {
                Statement statement = connection.createStatement();

                statement.executeUpdate(vaccination_query);
                count_instered_vaccinations++;

                statement.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }


        return count_instered_vaccinations;
    }

    @Override
    public int insertSeen(Seen[] seens) {
        int count_instered_seen=0;

        for(int i=0;i<seens.length;i++){
            String seen_query = "insert into seen values (\"" +
                    seens[i].getEffectcode() + "\",\"" +
                    seens[i].getCode() + "\",\"" +
                    seens[i].getUserID() + "\",\"" +
                    seens[i].getDate() + "\",\"" +
                    seens[i].getDegree() +"\")";


            try {
                Statement statement = connection.createStatement();

                statement.executeUpdate(seen_query);
                count_instered_seen++;

                statement.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }


        return count_instered_seen;
    }

    @Override
    public Vaccine[] getVaccinesNotAppliedAnyUser() {
        /*
        SELECT distinct v.code, v.vaccine_name, v.vaccine_type
        FROM vaccine v
        WHERE v.code NOT IN
                (SELECT distinct v.code
                FROM vaccine v, vaccination a
                WHERE v.code=a.code)
        order by v.code;*/

        ArrayList<Vaccine> vaccine_list = new ArrayList<Vaccine>();
        Vaccine vac[];
        Vaccine vac_new;
        ResultSet res;

        String query  = "SELECT distinct v.code, v.vaccine_name, v.vaccine_type " +
                "FROM vaccine v " +
                "WHERE v.code NOT IN (" +
                "SELECT distinct v.code " +
                "FROM vaccine v, vaccination a " +
                "WHERE v.code=a.code) " +
                "order by v.code;";
        try{
            Statement statement = connection.createStatement();
            res = statement.executeQuery(query);

            while(res.next()){
                int v_code = res.getInt("code");
                String v_vac_name = res.getString("vaccine_name");
                String v_type = res.getString("vaccine_type");
                vac_new = new Vaccine(v_code,v_vac_name,v_type);
                vaccine_list.add(vac_new);

            }


            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        vac = new Vaccine[vaccine_list.size()];
        vac = vaccine_list.toArray(vac);

        return vac;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersforTwoDosesByDate(String vacdate) {
        ArrayList<QueryResult.UserIDuserNameAddressResult> use_list = new ArrayList<QueryResult.UserIDuserNameAddressResult>();
        QueryResult.UserIDuserNameAddressResult use[];
        QueryResult.UserIDuserNameAddressResult  use_new;
        ResultSet res;

        String query  = "SELECT distinct u.userID, u.userName, u.address " +
                "FROM user u, vaccination a " +
                "WHERE a.userID=u.userID AND a.vac_date >= " + "\""+vacdate+"\""  +
                " GROUP BY u.userID " +
                "HAVING COUNT(*)=2 " +
                "order by u.userID;";
        try{
            Statement statement = connection.createStatement();
            res = statement.executeQuery(query);

            while(res.next()){
                String u_ID = res.getString("userID");
                String u_name = res.getString("userName");
                String u_address = res.getString("address");
                use_new = new QueryResult.UserIDuserNameAddressResult(u_ID,u_name,u_address);
                use_list.add(use_new);

            }


            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        use = new QueryResult.UserIDuserNameAddressResult[use_list.size()];
        use = use_list.toArray(use);

        return use;
    }

    @Override
    public Vaccine[] getTwoRecentVaccinesDoNotContainVac() {
        /*SELECT distinct v.code, v.vaccine_name, v.vaccine_type, MAX(a.vac_date)
FROM user u, vaccination a, vaccine v
WHERE u.userID = a.userID AND a.code=v.code AND v.vaccine_name NOT LIKE '%vac%'
GROUP BY v.code
order by MAX(a.vac_date);
*/      int j=0;
        ArrayList<Vaccine> vaccine_list = new ArrayList<Vaccine>();
        Vaccine vac[];
        Vaccine temp[] = new Vaccine[1];;
        Vaccine vac_new;
        ResultSet res;

        String query  = "SELECT distinct v.code, v.vaccine_name, v.vaccine_type, MAX(a.vac_date) " +
                "FROM user u, vaccination a, vaccine v " +
                "WHERE u.userID = a.userID AND a.code=v.code AND v.vaccine_name NOT LIKE '%vac%' " +
                "GROUP BY v.code " +
                "order by MAX(a.vac_date) desc;";
        try{
            Statement statement = connection.createStatement();
            res = statement.executeQuery(query);

            while(res.next()){
                if(j==2)
                    break;

                int v_code = res.getInt("code");
                String v_vac_name = res.getString("vaccine_name");
                String v_type = res.getString("vaccine_type");
                vac_new = new Vaccine(v_code,v_vac_name,v_type);
                vaccine_list.add(vac_new);
                j++;
            }


            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        vac = new Vaccine[vaccine_list.size()];
        vac = vaccine_list.toArray(vac);
        if(vaccine_list.size()==2  && (vac[0].getCode()>vac[1].getCode())){
            temp[0] = vac[0];
            vac[0] = vac[1];
            vac[1] = temp[0];
        }

        return vac;



    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersAtHasLeastTwoDoseAtMostOneSideEffect() {
        ArrayList<QueryResult.UserIDuserNameAddressResult> use_list = new ArrayList<QueryResult.UserIDuserNameAddressResult>();
        QueryResult.UserIDuserNameAddressResult use[];
        QueryResult.UserIDuserNameAddressResult  use_new;
        ResultSet res;

        String query = "SELECT distinct u.userID, u.userName, u.address " +
                "FROM user u,  vaccination a " +
                "WHERE u.userID = a.userID AND a.dose=2  AND u.userID NOT IN ( " +
                "    SELECT distinct u1.userID " +
                "    FROM seen s1,user u1 " +
                "    WHERE u1.userID=s1.userID " +
                "    GROUP BY s1.userID " +
                "    HAVING COUNT(*) > 1 " +
                ")"+
                "order by u.userID;";



        try{
            Statement statement = connection.createStatement();
            res = statement.executeQuery(query);

            while(res.next()){
                String u_ID = res.getString("userID");
                String u_name = res.getString("userName");
                String u_address = res.getString("address");
                use_new = new QueryResult.UserIDuserNameAddressResult(u_ID,u_name,u_address);
                use_list.add(use_new);

            }


            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        use = new QueryResult.UserIDuserNameAddressResult[use_list.size()];
        use = use_list.toArray(use);

        return use;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersWithAllVaccinesCanCauseGivenSideEffect(String effectname) {
        ArrayList<QueryResult.UserIDuserNameAddressResult> use_list = new ArrayList<QueryResult.UserIDuserNameAddressResult>();
        QueryResult.UserIDuserNameAddressResult use[];
        QueryResult.UserIDuserNameAddressResult  use_new;
        ResultSet res;

        String query = "SELECT distinct u.userID, u.userName,u.address " +
                "FROM user u " +
                "WHERE NOT EXISTS ( " +
                "    SELECT e.effect_code " +
                "    FROM AllergicSideEffect e, seen s " +
                "    WHERE e.effect_name =" + "\"" + effectname + "\""+ " AND s.effect_code=e.effect_code AND NOT EXISTS( " +
                "            SELECT a.code " +
                "            FROM vaccination a " +
                "            WHERE  a.userID = u.userID AND a.code = s.code " +
                "        ) " +
                "    )" +
                "order by u.userID;";


        try{
            Statement statement = connection.createStatement();
            res = statement.executeQuery(query);

            while(res.next()){
                String u_ID = res.getString("userID");
                String u_name = res.getString("userName");
                String u_address = res.getString("address");
                use_new = new QueryResult.UserIDuserNameAddressResult(u_ID,u_name,u_address);
                use_list.add(use_new);

            }


            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        use = new QueryResult.UserIDuserNameAddressResult[use_list.size()];
        use = use_list.toArray(use);

        return use;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersWithAtLeastTwoDifferentVaccineTypeByGivenInterval(String startdate, String enddate) {
        ArrayList<QueryResult.UserIDuserNameAddressResult> use_list = new ArrayList<QueryResult.UserIDuserNameAddressResult>();
        QueryResult.UserIDuserNameAddressResult use[];
        QueryResult.UserIDuserNameAddressResult  use_new;
        ResultSet res;

        String query  = "SELECT distinct u.userID, u.userName, u.address " +
                "FROM user u " +
                "WHERE u.userID IN (  " +
                "SELECT a1.userID " +
                "FROM vaccination a1, vaccination a2, vaccine v1, vaccine v2 " +
                "WHERE v1.code=a1.code AND a1.userID=a2.userID AND v2.code=a2.code AND v1.vaccine_type<>v2.vaccine_type AND " +
                "(a1.vac_date BETWEEN " + "\""+startdate+"\"" + "AND " + "\""+enddate+"\"" + " )AND " +
                "(a2.vac_date BETWEEN " + "\""+startdate+"\"" + "AND " + "\""+enddate+"\"" + "))" +
                "order by u.userID;";
        try{
            Statement statement = connection.createStatement();
            res = statement.executeQuery(query);

            while(res.next()){
                String u_ID = res.getString("userID");
                String u_name = res.getString("userName");
                String u_address = res.getString("address");
                use_new = new QueryResult.UserIDuserNameAddressResult(u_ID,u_name,u_address);
                use_list.add(use_new);

            }


            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        use = new QueryResult.UserIDuserNameAddressResult[use_list.size()];
        use = use_list.toArray(use);

        return use;




    }

    @Override
    public AllergicSideEffect[] getSideEffectsOfUserWhoHaveTwoDosesInLessThanTwentyDays() {
        /*
        SELECT e.effect_code,e.effect_name
FROM AllergicSideEffect e, seen s
WHERE s.effect_code = e.effect_code AND s.userID IN (
        SELECT a1.userID
        FROM vaccination a1, vaccination a2
        WHERE a1.userID = a2.userID AND a1.dose > a2.dose AND DATEDIFF(a1.vac_date,a2.vac_date) < 20
    )
         */
        ArrayList<AllergicSideEffect> aler_list = new ArrayList<AllergicSideEffect>();
        AllergicSideEffect aler[];
        AllergicSideEffect aler_new;
        ResultSet res;

        String query  = "SELECT distinct e.effect_code,e.effect_name " +
                "FROM AllergicSideEffect e, seen s " +
                "WHERE s.effect_code = e.effect_code AND s.userID IN ( " +
                " SELECT a1.userID " +
                " FROM vaccination a1, vaccination a2 " +
                " WHERE a1.userID = a2.userID AND a1.dose > a2.dose AND " +
                "DATEDIFF(a1.vac_date,a2.vac_date) < 20 ) " +
                "order by e.effect_code;";

        try{
            Statement statement = connection.createStatement();
            res = statement.executeQuery(query);

            while(res.next()){
                int a_effect_code = res.getInt("effect_code");
                String a_effect_name = res.getString("effect_name");

                aler_new = new AllergicSideEffect(a_effect_code,a_effect_name);
                aler_list.add(aler_new);

            }


            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        aler = new AllergicSideEffect[aler_list.size()];
        aler =aler_list.toArray(aler);

        return aler;


    }

    @Override
    public double averageNumberofDosesofVaccinatedUserOverSixtyFiveYearsOld() {
        /*
        SELECT AVG(a.dose)
        FROM User u, Vaccination a
        WHERE u.userID=a.userID AND u.age>65
         */
        double avg_dose=0;
        ResultSet res;
        String query = "SELECT AVG(Temp.max_ages) as avgdose " +
                "FROM (SELECT distinct u.userID, COUNT(*) as max_ages " +
                "FROM user u, vaccination a "+
                "WHERE u.age>65 AND u.userID = a.userID " +
                "GROUP BY u.userID ) as Temp;";

        try{
            Statement statement = connection.createStatement();
            res = statement.executeQuery(query);

            res.next();
            avg_dose = res.getDouble("avgdose");




            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return avg_dose;
    }

    @Override
    public int updateStatusToEligible(String givendate) {
        /*
        UPDATE user u
        SET u.status =  'eligible'
        WHERE  u.userID IN(
                    SELECT   a.userID
                    FROM  vaccination a
                    Group By a.userID
                    HAVING 120<DATEDIFF('2021-12-19',MAX(a.vac_date)))

        */


        int up[];
        int up_new;
        int affected_row=0;
        ResultSet res;

        String query = "UPDATE user u " +
                "SET u.status=\"eligible\" "+
                "WHERE  u.status <>\"eligible\" AND u.userID IN ( SELECT   a.userID " +
                "FROM  vaccination a" +
                " Group By a.userID " +
                "HAVING 120<DATEDIFF(" +
                "\"" + givendate + "\"" +
                ",MAX(a.vac_date)));";
        try{
            Statement statement = connection.createStatement();
            affected_row =  statement.executeUpdate(query);

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return affected_row;

    }

    @Override
    public Vaccine deleteVaccine(String vaccineName) {
        Vaccine vac = null;
        ResultSet res;

        String query = "SELECT * " +
                "FROM vaccine v " +
                "WHERE v.vaccine_name = " + "\"" + vaccineName + "\";";
        try{
            Statement statement = connection.createStatement();
            res = statement.executeQuery(query);

            res.next();
            int v_code = res.getInt("code");
            String v_name = res.getString("vaccine_name");
            String v_type = res.getString("vaccine_type");

            vac = new Vaccine(v_code,v_name,v_type);

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        String query1 = "DELETE FROM vaccine v "+
                "WHERE v.vaccine_name = " + "\"" + vaccineName + "\";";

        try{
            Statement statement = connection.createStatement();
            statement.executeUpdate(query1);

            statement.close();
        }
        catch (SQLException e) {
        e.printStackTrace();
    }

        return  vac;
    }




}
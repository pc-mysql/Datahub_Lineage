package com.ictrui.datahub_lineage.util;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat;
//import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class Jsonutil  {

    //  read the json


    // get the reader table and name.

    public static List<String> parsetable(JSONObject reader) {
        ArrayList<String> table = new ArrayList<>();
        JSONObject parameter = reader.getJSONObject("parameter");
        JSONArray jsonArray = parameter.getJSONArray("connection");
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject o = jsonArray.getJSONObject(i);
            if (o.getString("table") != null) {
                String tablename = o.getString("table");
                tablename = StringUtils.stripStart(tablename,"[");
                tablename = StringUtils.stripEnd(tablename,"]");
                table.add(tablename);
            }
            else{
                JSONArray sql = o.getJSONArray("querySql");
                List<String> strings = sql.toJavaList(String.class);
                for (String string : strings) {
                    log.info("querySql遇到的sql为{}",string);
                    table.addAll(parsesql(string));
                }
            }
        }
        return table;
    }

    private static List<String> parsesql(String sql) {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        // 使用Parser解析生成AST，这里SQLStatement就是AST
        SQLStatement sqlStatement = parser.parseStatement();
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        sqlStatement.accept(visitor);
        Map<TableStat.Name, TableStat> tables = visitor.getTables();
        List<String> allTableName = new ArrayList<>();
        for (TableStat.Name t : tables.keySet()) {
            allTableName.add(t.getName());
        }
        return allTableName;
    }


    public static boolean TestConnection(String host, int port)  {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host,port),3000);
            log.info("airflow地址和端口号可用");
        } catch (Exception e) {
            log.info("airflow地址和端口号不可用");

        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    System.out.println("1");
                }
            }
        }
        return true;
    }
}

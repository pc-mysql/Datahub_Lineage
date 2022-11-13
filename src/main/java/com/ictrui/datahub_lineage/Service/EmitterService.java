package com.ictrui.datahub_lineage.Service;

import com.alibaba.fastjson.JSONObject;
import com.ictrui.datahub_lineage.Entity.Json;
import com.ictrui.datahub_lineage.Entity.LinageSql;
import com.ictrui.datahub_lineage.Entity.Pythonemitter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

@Service
public class EmitterService {
    @Autowired
    private Json json;
    @Autowired
    private Pythonemitter pythonemitter;

    @Autowired
    private LinageSql linageSql;

    public EmitterService() {
    }



    public JSONObject sendque() {
        JSONObject jsonObject = new JSONObject();
        pythonemitter.check();
        pythonemitter.run(json);
        return jsonObject;


    }

    public void getjson(JSONObject jsonParam) {
        json.readJSON(jsonParam);
        json.log();
    }

    public void getsql(String sql) {
        linageSql.readsql(sql);
        linageSql.handlesql();
        linageSql.log();
    }
}

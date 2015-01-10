package com.github.sakserv.storm.scheme;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class JsonScheme implements Scheme {

    private static final long serialVersionUID = -2990121166902741545L;

    @Override
    public List<Object> deserialize(byte[] bytes) {

        int id = 0;
        String msg = null;
        String dt = null;
        try {
            String eventDetails = new String(bytes, "UTF-8");
            JSONObject obj = new JSONObject(eventDetails);
            id = Integer.valueOf(obj.getString("id"));
            msg = String.valueOf(obj.get("msg"));
            dt = String.valueOf(obj.get("dt"));
            return new Values(id, msg, dt);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return new Values(id, msg, dt);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("id", "msg", "dt");
    }
}

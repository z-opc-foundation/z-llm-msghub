package com.zifang.z.team.msg.persistence;

import com.zifang.util.json.JsonUtil;
import com.zifang.util.json.model.JsonArray;
import com.zifang.util.json.model.JsonObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * JSON 文件路由表持久化
 * <p>
 * 将订阅信息以 JSON 格式保存到本地文件：
 * - subs.json: 订阅信息（topic + sessionId）
 * <p>
 * 用于 msgHub-starter 独立部署时，重启恢复订阅。
 * 当前阶段（内存模式）不使用，为远端部署预留。
 */
public class JsonRouteStore implements RouteStore {

    private final String dataDir;

    public JsonRouteStore(String dataDir) {
        this.dataDir = dataDir != null ? dataDir : ".";
        ensureDir(this.dataDir);
    }

    @Override
    public void saveSubscriptions(List<SubscriptionSnapshot> subscriptions) {
        JsonArray arr = new JsonArray();
        for (SubscriptionSnapshot s : subscriptions) {
            JsonObject obj = new JsonObject();
            obj.put("topic", s.topic);
            obj.put("sessionId", s.sessionId);
            arr.add(obj);
        }
        writeJson("subs.json", arr);
    }

    @Override
    public List<SubscriptionSnapshot> loadSubscriptions() {
        JsonArray arr = readJsonArray("subs.json");
        List<SubscriptionSnapshot> result = new ArrayList<>();
        if (arr == null) return result;
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            SubscriptionSnapshot s = new SubscriptionSnapshot();
            s.topic = obj.getString("topic");
            s.sessionId = obj.getString("sessionId");
            result.add(s);
        }
        return result;
    }

    // ========== 内部工具 ==========

    private void ensureDir(String dir) {
        File f = new File(dir);
        if (!f.exists()) {
            f.mkdirs();
        }
    }

    private void writeJson(String fileName, JsonArray arr) {
        File file = new File(dataDir, fileName);
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8)) {
            writer.write(JsonUtil.toJsonPretty(arr));
        } catch (IOException e) {
            throw new RuntimeException("Failed to write " + fileName, e);
        }
    }

    private JsonArray readJsonArray(String fileName) {
        File file = new File(dataDir, fileName);
        if (!file.exists()) {
            return null;
        }
        try {
            String content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
            return JsonUtil.parseArray(content);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + fileName, e);
        }
    }
}

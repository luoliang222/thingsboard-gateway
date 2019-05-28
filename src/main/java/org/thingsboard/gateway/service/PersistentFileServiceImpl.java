/**
 * Copyright © 2017 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.gateway.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.thingsboard.gateway.service.conf.TbPersistenceConfiguration;
import org.thingsboard.gateway.util.SnowflakeIdWorker;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Created by Valerii Sosliuk on 1/2/2018.
 */
@Slf4j
public class PersistentFileServiceImpl implements PersistentFileService {

    private static final String STORAGE_FILE_PREFIX = "tb-gateway-storage-";
    private static final String RESEND_FILE_PREFIX = "tb-gateway-resend-";
    private static final String STORAGE_FILE_NAME_REGEX = STORAGE_FILE_PREFIX + "\\d+";
    private static final String RESEND_FILE_NAME_REGEX = RESEND_FILE_PREFIX + "\\d+";
    public static final String DASH = "-";

    private TbPersistenceConfiguration persistence;
    private String tenantName;

    private ConcurrentLinkedDeque<MqttPersistentMessage> sendBuffer;
//    private ConcurrentLinkedDeque<MqttPersistentMessage> resendBuffer;

    private ConcurrentMap<UUID, MqttCallbackWrapper> callbacks;
    private Map<UUID, MqttDeliveryFuture> futures;

//    private List<File> storageFiles;
    private LinkedHashMap<UUID, File> resendFilesMap = new LinkedHashMap<UUID, File>();

//    private int storageFileCounter;
//    private int resendFileCounter;
    private long lastRefreshMil = 0;

    private File storageDir;
    private SnowflakeIdWorker snowFlake = new SnowflakeIdWorker(0, 0);

    public long getLastRefreshMil(){
        return lastRefreshMil;
    }

    @PostConstruct
    public void init() {
        callbacks = new ConcurrentHashMap<>();
        futures = new ConcurrentHashMap<>();
        initStorageDir();
        initFiles();
        initFileCounters();
        initBuffers();
    }

    private void initStorageDir() {
        String storageSubdir = tenantName.replaceAll(" ", "_");
        storageDir = new File(persistence.getPath(), storageSubdir);
        if (!storageDir.exists()) {
            storageDir.mkdirs();
        }
    }

    private void initFiles() {
//        storageFiles = getFiles(STORAGE_FILE_NAME_REGEX);
        List<File> l = getFiles(STORAGE_FILE_NAME_REGEX);
        for (File file : l) {
            String fname = file.getName();
            long id = Long.parseLong(fname.substring(fname.lastIndexOf(DASH) + 1));
            if (id != Long.MAX_VALUE) {
                UUID uid = new UUID(0, id);
                resendFilesMap.put(uid, file);
            }
        }
    }

    private List<File> getFiles(String nameRegex) {
        File[] filesArray = storageDir.listFiles((file) -> {return !file.isDirectory() && file.getName().matches(nameRegex);});
        Arrays.sort(filesArray, Comparator.comparing(File::lastModified));
        return new ArrayList<>(Arrays.asList(filesArray));
    }

    private void initFileCounters() {
//        storageFileCounter = getFileCounter(storageFiles);
//        resendFileCounter = getFileCounter(resendFiles);
    }

    private int getFileCounter(List<File> files) {
        int counter = 0;
        if (files.isEmpty()) {
            return counter;
        } else {
            String lastFileName = files.get(files.size() - 1).getName();
            int lastFileCounter = Integer.parseInt(lastFileName.substring(lastFileName.lastIndexOf(DASH) + 1));
            if (lastFileCounter == Integer.MAX_VALUE) {
                // rename all storageFiles if overflow?
                counter = 0;
            } else {
                counter = lastFileCounter + 1;
            }
            return counter;
        }
    }

    private void initBuffers() {
        sendBuffer = new ConcurrentLinkedDeque<>();
//        resendBuffer = new ConcurrentLinkedDeque<>();
    }

    @Override
    public MqttDeliveryFuture persistMessage(String topic,  int msgId, byte[] payload, String deviceId,
                                             Consumer<Void> onSuccess,
                                             Consumer<Throwable> onFailure) throws IOException {
        UUID uuid = new UUID(0, snowFlake.nextId());
        MqttPersistentMessage message = MqttPersistentMessage.builder().id( uuid )
                .topic(topic).deviceId(deviceId).messageId(msgId).payload(payload).build();
        MqttDeliveryFuture future = new MqttDeliveryFuture();
        addMessageToBuffer(message);
        callbacks.put(message.getId(), new MqttCallbackWrapper(onSuccess, onFailure));
        futures.put(message.getId(), future);
        return future;
    }

    @Override
    public List<MqttPersistentMessage> getPersistentMessages() throws IOException {
        List<MqttPersistentMessage> messages;
        messages = new ArrayList<>(sendBuffer);
        sendBuffer.clear();
        return messages;
    }

    @Override
    public List<MqttPersistentMessage> getResendMessages() throws IOException {
        List<MqttPersistentMessage> messages = new ArrayList<MqttPersistentMessage>();
        int maxReturn = 1000;
        if (resendFilesMap.size() > 0){
            if (sendBuffer.size() > 0){
                // 将内存缓冲sendBuffer的数据移动到 ==> resend，以防止数据丢失
                saveForResend(getPersistentMessages());
            }

            ListIterator<Map.Entry<UUID, File>> reverseIte = new ArrayList<Map.Entry<UUID, File>>
                    (resendFilesMap.entrySet()).listIterator(resendFilesMap.size());
            while (reverseIte.hasPrevious()) {
                if (maxReturn -- < 0)
                    break;
                Map.Entry<UUID, File> it = reverseIte.previous();
                File file = it.getValue();
                // 读取列表中的所有文件 ==> messages
                messages.addAll(readFromFile(file));

                // 从resendFilesMap 中移除该记录
                resendFilesMap.remove(it.getKey());
            }
        }
        return messages;
    }

    private File newFileById(long id){
        String idString = Long.toString(id);    //Long.toHexString(id);
        File newFile = new File(storageDir, STORAGE_FILE_PREFIX + idString);
        return newFile;
    }

    @Override
    public void resolveFutureSuccess(UUID id) {
        callbacks.remove(id);

        // 删除缓存文件、队列中的项
        File f = newFileById(id.getLeastSignificantBits());
        if (f != null)
            f.delete();

        MqttDeliveryFuture future = futures.remove(id);
        if (future != null) {
            future.complete(Boolean.TRUE);
        }
    }

    @Override
    public void resolveFutureFailed(UUID id, Throwable e) {

        // 外部已经执行重发了！
        // 发送失败，需要重发
//        File f = newFileById(id.getLeastSignificantBits());
//        if (f.exists())
//            resendFilesMap.put(id, f);
//        else
//            log.error("resolveFutureFailed: file not exist of message id:" + id);

        MqttDeliveryFuture future = futures.remove(id);
        if (future != null) {
            future.completeExceptionally(e);
        }
    }

    @Override
    public Optional<MqttDeliveryFuture> getMqttDeliveryFuture(UUID id) {
        return Optional.of(futures.get(id));
    }

    @Override
    public boolean deleteMqttDeliveryFuture(UUID id) {
        return futures.remove(id) != null;
    }

    @Override
    public Optional<Consumer<Void>> getSuccessCallback(UUID id) {
        MqttCallbackWrapper mqttCallbackWrapper = callbacks.get(id);
        if (mqttCallbackWrapper == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(mqttCallbackWrapper.getSuccessCallback());
    }

    @Override
    public Optional<Consumer<Throwable>> getFailureCallback(UUID id) {
        MqttCallbackWrapper mqttCallbackWrapper = callbacks.get(id);
        if (mqttCallbackWrapper == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(mqttCallbackWrapper.getFailureCallback());
    }

    private boolean isRefreshTimeout(){
        return (System.currentTimeMillis() > (getLastRefreshMil() + persistence.getRefrashInterval()));
    }

    // 保存消息到存储列表
    // resendBuffer 为 id --> File
    // save时将消息写入文件 /storage/id
    @Override
    public void saveForResend(MqttPersistentMessage message) throws IOException {
        File f = flushBufferToFile(message);
        resendFilesMap.put(message.getId(), f);
    }

    @Override
    public void saveForResend(List<MqttPersistentMessage> messages) throws IOException {
       for (MqttPersistentMessage message : messages) {
           saveForResend(message);
       }
    }

    // sendBuffer 为内存buffer
    private void addMessageToBuffer(MqttPersistentMessage message) throws IOException {
        if (resendFilesMap.size() == 0)
            sendBuffer.add(message);
        else
            saveForResend(message);
    }

    // 将消息写入文件，每个消息一个独立文件，文件名为 STORAGE_FILE_PREFIX + id
    private File flushBufferToFile(MqttPersistentMessage message) throws IOException {
        long id = message.getId().getLeastSignificantBits();
        File newFile = newFileById(id);
        if (newFile.exists())
            return newFile;     // 文件已经存在缓存中，不需要再写

        ObjectOutputStream outStream = null;
        lastRefreshMil = System.currentTimeMillis();
        try {
            outStream = new ObjectOutputStream(new FileOutputStream(newFile));
            outStream.writeObject(message);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            try {
                if (outStream != null)
                    outStream.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                throw e;
            }
        }

        return newFile;
    }

    // 从文件读取一条消息
    private List<MqttPersistentMessage> readFromFile(File file)  throws IOException {
        List<MqttPersistentMessage> messages = new ArrayList<>();
        ObjectInputStream inputStream = null;
        try {
            inputStream = new ObjectInputStream(new FileInputStream(file));
            while (true) {
                MqttPersistentMessage p = (MqttPersistentMessage) inputStream.readObject();
                messages.add(p);
            }
        } catch (EOFException e) {
            return messages;
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            try {
                if (inputStream != null)
                    inputStream.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                throw e;
            }
        }
        return messages;
    }

    public void setPersistence(TbPersistenceConfiguration persistence) {
        this.persistence = persistence;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }
}

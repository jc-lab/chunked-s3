package kr.jclab.cloud.chunkeds3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.jclab.cloud.chunkeds3.dto.ObjectMetadataDTO;
import org.apache.http.HttpStatus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ChunkedS3 {
    private static final String S3MetadataKey = "kr.jclab.cloud.chunkeds3";

    private AmazonS3 s3client;
    private final String bucket;

    private String hashAlgorithm = null;
    private MessageDigest messageDigest = null;
    private int chunkSize = 33554432; // 32MiB

    private ObjectMapper objectMapper = new ObjectMapper();

    public ChunkedS3(AmazonS3 s3client, String bucket) {
        this.s3client = s3client;
        this.bucket = bucket;
        try {
            setHashAlgorithm("SHA-1");
        } catch (NoSuchAlgorithmException e) {
        }
    }

    public String getHashAlgorithm() {
        return hashAlgorithm;
    }

    public void setHashAlgorithm(String hashAlgorithm) throws NoSuchAlgorithmException {
        this.hashAlgorithm = hashAlgorithm;
        messageDigest = MessageDigest.getInstance(hashAlgorithm);
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        if(chunkSize % 8192 != 0) {
            this.chunkSize += chunkSize % 8192;
        }
    }

    public void putObject(String name, InputStream inputStream) throws NoSuchAlgorithmException, IOException, SdkClientException, AmazonServiceException {
        ObjectMetadataDTO objectMetadataDTO = new ObjectMetadataDTO();
        int chunkIndex = 0;
        PutChunkContext putObjectResult = new PutChunkContext();
        MessageDigest mdFile = MessageDigest.getInstance(this.messageDigest.getAlgorithm(), this.messageDigest.getProvider());
        MessageDigest mdChunk = MessageDigest.getInstance(this.messageDigest.getAlgorithm(), this.messageDigest.getProvider());
        ObjectMetadata masterObjectMetadata = new ObjectMetadata();
        ByteArrayInputStream masterObjectInputStream;

        while (!putObjectResult.isCompleted()) {
            ObjectMetadata chunkObjectMetadata = new ObjectMetadata();
            PutChunkInputStream chunkInputStream = new PutChunkInputStream(putObjectResult, inputStream, mdFile, mdChunk);
            PutObjectResult putResult;
            chunkObjectMetadata.addUserMetadata("kr.jclab.cloud.ms3.directoryspliter", "true");
            putResult = s3client.putObject(bucket, chunkName(name, chunkIndex), chunkInputStream, chunkObjectMetadata);
            objectMetadataDTO.addChunkInfo(new ObjectMetadataDTO.ChunkInfo(bytes2hex(mdChunk.digest()), (int)chunkInputStream.getTotalReadSize()));
            chunkIndex++;
        }

        objectMetadataDTO.setFileHashAlgorithm(mdFile.getAlgorithm());
        objectMetadataDTO.setFileHashValue(bytes2hex(mdFile.digest()));
        objectMetadataDTO.setFileSize(putObjectResult.getCurrentPos());

        masterObjectMetadata.addUserMetadata(S3MetadataKey, "true");

        masterObjectInputStream = new ByteArrayInputStream(objectMapper.writeValueAsString(objectMetadataDTO).getBytes("UTF-8"));
        s3client.putObject(bucket, name, masterObjectInputStream, masterObjectMetadata);
    }

    public ObjectMetadataDTO getObjectMetadata(String name) throws IOException {
        try {
            S3Object s3Object = s3client.getObject(this.bucket, name);
            try {
                ObjectMetadata masterObjectMetadata = s3Object.getObjectMetadata();
                if(masterObjectMetadata != null) {
                    String metadataValue = masterObjectMetadata.getUserMetaDataOf(S3MetadataKey);
                    if("true".equalsIgnoreCase(metadataValue) || "1".equalsIgnoreCase(metadataValue)) {
                        return objectMapper.readValue(s3Object.getObjectContent(), ObjectMetadataDTO.class);
                    }
                }
                return null;
            } catch (IOException e) {
                try {
                    s3Object.close();
                } catch (IOException ne) {
                }
                throw e;
            }
        } catch (AmazonS3Exception s3Exception) {
            if(s3Exception.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return null;
            }
            throw s3Exception;
        }
    }

    public boolean deleteObject(String name) throws IOException {
        try {
            ObjectMetadataDTO objectMetadataDTO = getObjectMetadata(name);
            if(objectMetadataDTO == null) {
                return false;
            }
            s3client.deleteObject(this.bucket, name);
            for (int i = 0, count = objectMetadataDTO.getChunkCount(); i < count; i++) {
                s3client.deleteObject(this.bucket, chunkName(name, i));
            }
            return true;
        } catch (AmazonS3Exception e) {
            if(e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return false;
            }
            throw e;
        }
    }

    public S3Object getChunkS3Object(String name, int chunkIndex) throws SdkClientException, AmazonServiceException {
        return this.s3client.getObject(this.bucket, chunkName(name, chunkIndex));
    }

    private String chunkName(String name, int chunkIndex) {
        int i=0;
        StringBuilder stringBuilder = new StringBuilder();
        String hexIndex = Integer.toUnsignedString(chunkIndex, 16);
        stringBuilder.append(name);
        stringBuilder.append("-CHUNK_");
        for(i=8 - hexIndex.length(); i > 0; i--) {
            stringBuilder.append("0");
        }
        stringBuilder.append(hexIndex);
        return stringBuilder.toString();
    }

    private String bytes2hex(byte[] data) {
        final String HexString = "0123456789abcdef";
        StringBuilder stringBuilder = new StringBuilder();
        for(int i=0, count = data.length; i<count; i++) {
            stringBuilder.append(HexString.charAt((data[i] >> 4) & 0xF));
            stringBuilder.append(HexString.charAt((data[i] >> 0) & 0xF));
        }
        return stringBuilder.toString();
    }

    private class PutChunkContext {
        private long currentPos = 0;
        private boolean completed = false;

        public long getCurrentPos() {
            return currentPos;
        }

        public void addCurrentPos(long size) {
            this.currentPos += size;
        }

        public void setCompleted() {
            completed = true;
        }

        public boolean isCompleted() {
            return completed;
        }
    }

    private class PutChunkInputStream extends InputStream {
        private final InputStream realInputStream;
        private final PutChunkContext putChunkContext;
        private final MessageDigest mdFile;
        private final MessageDigest mdChunk;

        private long totalReadSize = 0;

        public PutChunkInputStream(PutChunkContext putChunkContext, InputStream realInputStream, MessageDigest mdFile, MessageDigest mdChunk) {
            this.realInputStream = realInputStream;
            this.putChunkContext = putChunkContext;
            this.mdFile = mdFile;
            this.mdChunk = mdChunk;
            this.mdChunk.reset();
        }

        @Override
        public int read() throws IOException {
            long remainSize = remainSize();
            if(remainSize > 0) {
                int d = this.realInputStream.read();
                if (d < 0) {
                    this.putChunkContext.setCompleted();
                    return d;
                }
                this.mdFile.update((byte) d);
                this.mdChunk.update((byte) d);
                this.afterRead(1);
                return d;
            }
            return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            long remainSize = remainSize();
            if(remainSize > 0) {
                int readSize = (int)(len > remainSize ? remainSize : len);
                int readLen = this.realInputStream.read(b, off, readSize);
                if(readLen <= 0) {
                    this.putChunkContext.setCompleted();
                    return readLen;
                }
                this.mdFile.update(b, off, readLen);
                this.mdChunk.update(b, off, readLen);
                this.afterRead(readLen);
                return readLen;
            }
            return 0;
        }

        private long remainSize() {
            return chunkSize - totalReadSize;
        }

        private void afterRead(int readSize) {
            this.totalReadSize += readSize;
            this.putChunkContext.addCurrentPos(readSize);
        }

        public long getTotalReadSize() {
            return totalReadSize;
        }
    }
}

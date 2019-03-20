package kr.jclab.cloud.chunkeds3.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class ObjectMetadataDTO {
    @JsonProperty("file_hash_algorithm")
    private String fileHashAlgorithm;
    @JsonProperty("file_hash_value")
    private String fileHashValue;
    @JsonProperty("chunk_list")
    private List<ChunkInfo> chunkList = new ArrayList<>();
    @JsonProperty("file_size")
    private Long fileSize;

    public static class ChunkInfo {
        @JsonProperty("hash")
        private String hash;
        @JsonProperty("size")
        private Integer size;

        @JsonCreator
        public ChunkInfo(@JsonProperty("hash") String hash, @JsonProperty("size") Integer size) {
            this.hash = hash;
            this.size = size;
        }

        @JsonGetter("hash")
        public String getHash() {
            return hash;
        }

        @JsonGetter("size")
        public Integer getSize() {
            return size;
        }
    }

    public static class DataInvalidException extends RuntimeException {
        public DataInvalidException() {
            super();
        }

        public DataInvalidException(String message) {
            super(message);
        }

        public DataInvalidException(String message, Throwable cause) {
            super(message, cause);
        }

        public DataInvalidException(Throwable cause) {
            super(cause);
        }

        protected DataInvalidException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }

    public ObjectMetadataDTO() {
    }

    @JsonCreator
    public ObjectMetadataDTO(
            @JsonProperty("file_hash_algorithm") String fileHashAlgorithm,
            @JsonProperty("file_hash_value") String fileHashValue,
            @JsonProperty("chunk_list") List<ChunkInfo> chunkList,
            @JsonProperty("file_size") Long fileSize,
            @JsonProperty("chunk_count") int chunkCount) {
        this.fileHashAlgorithm = fileHashAlgorithm;
        this.fileHashValue = fileHashValue;
        this.chunkList = chunkList;
        this.fileSize = fileSize;
        if(this.chunkList.size() != chunkCount) {
            throw new DataInvalidException("Number of chunkHashes is not equals chunkCount");
        }
    }

    public String getFileHashAlgorithm() {
        return fileHashAlgorithm;
    }

    @JsonIgnore
    public void setFileHashAlgorithm(String fileHashAlgorithm) {
        this.fileHashAlgorithm = fileHashAlgorithm;
    }

    public String getFileHashValue() {
        return fileHashValue;
    }

    @JsonIgnore
    public void setFileHashValue(String fileHashValue) {
        this.fileHashValue = fileHashValue;
    }

    @JsonIgnore
    public void addChunkInfo(ChunkInfo chunkInfo) {
        this.chunkList.add(chunkInfo);
    }

    @JsonIgnore
    public List<ChunkInfo> getChunkHashes() {
        return this.chunkList;
    }

    @JsonIgnore
    public ChunkInfo getChunkInfo(int chunkIndex) {
        return this.chunkList.get(chunkIndex);
    }

    @JsonGetter("chunk_count")
    public int getChunkCount() {
        return this.chunkList.size();
    }

    @JsonProperty("file_size")
    public Long getFileSize() {
        return fileSize;
    }

    @JsonProperty("file_size")
    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }
}

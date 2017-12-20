#pragma once
#include <fstream>
#include <vector>
#include "mapreduce_spec.h"


typedef struct __perFileInfo
{
  std::string fileName;
  uint32_t startPos;
  uint32_t length;
}PerFileInfo;

struct FileShard {
  std::vector<PerFileInfo> fileInfo;
  uint32_t shardNum;
};


inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards);

typedef struct __myFileInfo
{
  std::fstream* file;
  uint32_t size;
}myFileInfo;


inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {

  uint32_t totalSize=0;
  std::vector<myFileInfo> files;

  files.resize(mr_spec.fileNames.size());

  for (int i = 0;i<mr_spec.fileNames.size();i++)
  {
    files[i].file = new std::fstream (mr_spec.fileNames[i].c_str(),std::fstream::in);
    if (!(*files[i].file))
    {
      std::cerr << "Unable to open file " << mr_spec.fileNames[i] << std::endl;
      exit(1);
    }
    files[i].file->seekg(0,files[i].file->end);
    files[i].size = files[i].file->tellg();
    totalSize += files[i].size;
    files[i].file->seekg(0,files[i].file->beg);
  }

  uint32_t sizePerPart = mr_spec.sizeKb * 1024;
  int remainingSize = totalSize;
  uint32_t shardCount = 0;
  uint32_t fileNum=0;
  FileShard tempShard;
  while(remainingSize > 0)
  {
    int remainingShardSize = (remainingSize > sizePerPart) ? sizePerPart : remainingSize;
    tempShard.shardNum = shardCount++;
    tempShard.fileInfo.clear();
    while(remainingShardSize > 0)
    {
      int cmpVal = (int) files[fileNum].size - files[fileNum].file->tellg() ;
      if (!cmpVal)
      {
        fileNum ++;
        continue;
      }
      if (cmpVal > remainingShardSize) //Case where the file is enough
      {
        std::string dummyRead;
        PerFileInfo tempVal;
        tempVal.fileName = mr_spec.fileNames[fileNum];
        tempVal.startPos = files[fileNum].file->tellg();
        files[fileNum].file->seekg(tempVal.startPos + remainingShardSize);
        std::getline(*(files[fileNum].file),dummyRead);
        tempVal.length = (uint32_t)((int)files[fileNum].file->tellg()- (int)tempVal.startPos);
        remainingShardSize = 0;
        remainingSize -= tempVal.length;
        tempShard.fileInfo.push_back(tempVal);
      }
      else
      {
        std::string dummyRead;
        PerFileInfo tempVal;
        tempVal.fileName = mr_spec.fileNames[fileNum];
        tempVal.startPos = files[fileNum].file->tellg();
        tempVal.length = files[fileNum].size - tempVal.startPos;
        files[fileNum].file->seekg(0,files[fileNum].file->end);
        remainingShardSize -= tempVal.length;
        remainingSize -= tempVal.length;
        tempShard.fileInfo.push_back(tempVal);
      }
    }
    fileShards.push_back(tempShard);
  }
  return true;
}


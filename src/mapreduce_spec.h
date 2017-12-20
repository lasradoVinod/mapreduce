#pragma once
#include <iostream>
#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <fstream>
#include <vector>
#include <sstream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	uint16_t numWorkers;
	std::vector<std::string> ipAddresses;
	std::vector<std::string> fileNames;
	std::string outputDirName;
	uint32_t numOutputFiles;
	uint32_t sizeKb;
	std::string user_id;	
};

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec);

/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec);

/*Custom code*/
inline std::vector<std::string> split(const std::string& s, char delimiter)
{
   std::vector<std::string> tokens;
   std::string token;
   std::istringstream tokenStream(s);
   while (std::getline(tokenStream, token, delimiter))
   {
      tokens.push_back(token);
   }
   return tokens;
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
  std::ifstream configFile;
  configFile.open(config_filename.c_str());
  std::string line;

  if (!configFile)
  {
      std::cerr << "Unable to open file Config File" << std::endl;
      exit(1);
  }
  while (! configFile.eof() )
  {
    std::getline (configFile,line);
    std::vector<std::string> temp = split(line,'=');
    if (temp.size() < 2)
      continue;
    if (!temp[0].compare("n_workers"))
    {
      mr_spec.numWorkers = atoi(temp[1].c_str());
    }
    if (!temp[0].compare("worker_ipaddr_ports"))
    {
      mr_spec.ipAddresses = split(temp[1],',');
    }
    if (!temp[0].compare("input_files"))
    {
      mr_spec.fileNames = split(temp[1],',');
      for (uint32_t i=0;i<mr_spec.fileNames.size();i++)
      {
        mr_spec.fileNames[i] = "."+mr_spec.fileNames[i];
      }
    }
    if (!temp[0].compare("output_dir"))
    {
      mr_spec.outputDirName = temp[1];
    }
    if (!temp[0].compare("n_output_files"))
    {
      mr_spec.numOutputFiles = atoi(temp[1].c_str());
    }
    if (!temp[0].compare("map_kilobytes"))
    {
      mr_spec.sizeKb = atoi(temp[1].c_str());
    }
    if (!temp[0].compare("user_id"))
    {
      mr_spec.user_id = temp[1];
    }
  }
  return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
  bool valid = true;
  struct stat s = {0};
  
  // Check number of workers
  if (mr_spec.numWorkers != mr_spec.ipAddresses.size()) valid = false;
  // Check if output directory exists
  else if (stat(mr_spec.outputDirName.c_str(), &s) || S_ISDIR(s.st_mode) == 0) valid = false;
  // Check if input files exists
  else {
   for (auto it = mr_spec.fileNames.begin(); it != mr_spec.fileNames.end(); it++){
      struct stat ss = {0};
      if ((stat(it->c_str(), &ss) != 0) || S_ISREG(ss.st_mode) == 0) {
         valid = false;
         break;
      }
   }
  }
  return valid;
}

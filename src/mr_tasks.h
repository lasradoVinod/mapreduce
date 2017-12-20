#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <string>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
      std::string out_file_prefix; // prefix of intermediate files
      unsigned max_out_files; // Max number of intermediate files that can be generated
      unsigned hash_gen(const std::string& s);
      std::map<std::string, bool> out_file_map;
      std::vector<std::string> file_list;

    public:
      void set_out_file_prefix(std::string filename); // fileName gives absolute path
      void set_max_out_files(unsigned max); // Set the max number of intermediate files to be generated
      std::vector<std::string> get_out_file_list();
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {
  max_out_files = 0;
}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
   // Generate hash value for input key
   unsigned key_hash = hash_gen(key) % max_out_files;

   // Get file mapping for current key
   std::string out_file_name = out_file_prefix + std::to_string(key_hash);
   std::ofstream ofs;
   ofs.open(out_file_name, std::ofstream::app);
   if (ofs.is_open() == false){
      printf("mr_tasks.h: Error while Mapping. Can't open intermediate file %s to write to. Aborting!\n", out_file_name.c_str());
      exit(0);
   }
   std::string data = key + " " + val + "\n";
   ofs.write(data.c_str(), data.size());
   if(out_file_map.find(out_file_name) == out_file_map.end()){
      out_file_map[out_file_name] = true;
   }
   ofs.close();
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
      std::string out_file_prefix; // prefix of intermediate files
      unsigned max_out_files; // Max number of intermediate files that can be generated
      std::map<std::string, bool> out_file_map;
      std::vector<std::string> file_list;

    public:
      void set_out_file_prefix(std::string filename); // fileName gives absolute path
      void set_max_out_files(unsigned max); // Set the max number of intermediate files to be generated
      std::vector<std::string> get_out_file_list();
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {
  max_out_files = 0;
}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
   std::string out_file_name = out_file_prefix;
   std::ofstream ofs;
   ofs.open(out_file_name, std::ofstream::app);
   if (ofs.is_open() == false){
      printf("mr_tasks.h: Error while Mapping. Can't open output file %s to write to. Aborting!\n", out_file_name.c_str());
      exit(0);
   }
   std::string data = key + " " + val + "\n";
   ofs.write(data.c_str(), data.size());
   if(out_file_map.find(out_file_name) == out_file_map.end()){
      out_file_map[out_file_name] = true;
   }
   ofs.close();
}

inline void BaseMapperInternal::set_out_file_prefix(std::string fileName){
   out_file_prefix = fileName;
}

inline void BaseMapperInternal::set_max_out_files(unsigned max){
   max_out_files = max;
}

inline unsigned BaseMapperInternal::hash_gen(const std::string& s){
   unsigned hash= (unsigned) s.at(0);
   return hash;
}

inline std::vector<std::string> BaseMapperInternal::get_out_file_list(){
   std::vector<std::string> list;
   for (auto it = out_file_map.begin(); it != out_file_map.end(); ++it){
      std::string key = it->first;
      list.push_back(key);
   }
   return list;
}

inline void BaseReducerInternal::set_out_file_prefix(std::string fileName){
   out_file_prefix = fileName;
}

inline void BaseReducerInternal::set_max_out_files(unsigned max){
   max_out_files = max;
}

inline std::vector<std::string> BaseReducerInternal::get_out_file_list(){
   std::vector<std::string> list;
   for (auto it = out_file_map.begin(); it != out_file_map.end(); ++it){
      std::string key = it->first;
      list.push_back(key);
   }
   return list;
}

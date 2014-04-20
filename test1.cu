#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct MapFileList {
	char* filename;
	struct MapFileList* next;
}MapFileList;

typedef struct MapInputList{
	char* key;
	char* value;
	struct MapInputList* next;
}MapInputList;

typedef enum InputFormat{TextInputFormat,KeyValueInputFormat,SequenceFileInputFormat} input_format;

typedef struct MapReduceSpec{
	MapFileList* map_file_list;
	MapInputList* map_input_list;
	unsigned map_block_num;
	unsigned map_thread_num;
	input_format map_input_format;
}MapReduceSpec;

void init_map_file_list(MapFileList* list){
	list->filename=NULL;
	list->next=NULL;
}

void free_map_file_list(MapFileList* list){
	MapFileList* del;
	MapFileList* tmp;
	del=list;
	tmp=list->next;
	while(tmp){
		if(del->filename!=NULL)
			free(del->filename);
		free(del);
		del=tmp;
		tmp=tmp->next;
	}
	if(del->filename!=NULL)
		free(del->filename);
	free(del);	
}

void init_map_input_list(MapInputList* list){
	list->key=NULL;
	list->value=NULL;
	list->next=NULL;
}

void free_map_input_list(MapInputList* list){
	MapInputList* del;
	MapInputList* tmp;
	del=list;
	tmp=list->next;
	while(tmp){
		if(del->key!=NULL)
			free(del->key);
		if(del->value!=NULL)
			free(del->value);
		free(del);
		del=tmp;
		tmp=tmp->next;
	}
	if(del->key!=NULL)
		free(del->key);
	if(del->value!=NULL)
		free(del->value);
	free(del);	
}

void init_mapreduce_spec(MapReduceSpec* spec){
	spec->map_file_list=NULL;
	spec->map_input_list=NULL;
	spec->map_block_num=4;
	spec->map_thread_num=128;
	spec->map_input_format=TextInputFormat;
}

void free_spec(MapReduceSpec* spec){
	free_map_file_list(spec->map_file_list);
	free_map_input_list(spec->map_input_list);
	free(spec);
}

void map_input_split(MapReduceSpec* spec){
	MapFileList* file_list_entry;
	size_t buffer_size=(size_t)256*1024*1024;
	size_t buffer_used=0;

	MapInputList* input_list_entry;

	FILE* pFile;
	file_list_entry=spec->map_file_list;
	
	input_list_entry=(MapInputList*)malloc(sizeof(MapInputList));
	init_map_input_list(input_list_entry);
	spec->map_input_list=input_list_entry;
	
	if(spec->map_input_format==TextInputFormat){
		size_t file_size;
		while(file_list_entry->filename!=NULL){
			pFile=fopen(file_list_entry->filename,"rb");
			if (pFile==NULL) {fputs ("File error\n",stderr); exit (1);}
			fseek (pFile , 0 , SEEK_END);
			file_size = ftell (pFile);
			rewind (pFile);
			if(buffer_used+file_size<=buffer_size){
				ssize_t result=0;
				size_t len = 0;
				while (result!= -1) {
					input_list_entry->key=(char*)malloc(10);
					sprintf(input_list_entry->key,"%d",(int)ftell(pFile));
					printf("%s ",input_list_entry->key);
					result=getline(&(input_list_entry->value), &len, pFile);
					printf("%s \n",input_list_entry->value);
					input_list_entry->next=(MapInputList*)malloc(sizeof(MapInputList));
					input_list_entry=input_list_entry->next;
					init_map_input_list(input_list_entry);
				}
				buffer_used=buffer_used+file_size;
			}
			else
				printf("Buffer full!!\n");
			file_list_entry=file_list_entry->next;
			fclose(pFile);
		}
	}


}

void add_input(char *path,MapReduceSpec* spec){
	MapFileList* plist;
	plist=(MapFileList*)malloc(sizeof(MapFileList));
	spec->map_file_list=plist;
	struct dirent* entry = NULL;
	DIR *pDir;
	pDir=opendir(path);
	while((entry=readdir(pDir))!=NULL){
		if(entry->d_type==DT_REG){
			plist->filename=(char*)malloc(strlen(path)+strlen(entry->d_name)+1);
			strcpy(plist->filename,path);
		       	strcat(plist->filename,entry->d_name);
			plist->next=(MapFileList*)malloc(sizeof(MapFileList));
			plist=plist->next;
		}
	}       
	map_input_split(spec);	
}

int main(int argc, char **argv){
	MapReduceSpec* spec=(MapReduceSpec*)malloc(sizeof(MapReduceSpec));
	add_input(argv[1],spec);
	free(spec);
}

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>

#define MAP_COUNT __device__ void mapCount(char*key,char*value,size_t key_size, size_t value_size,int*key_im_size,int*value_im_size,int*map_im_num,int threadID)
#define EMIT_IM_COUNT(im_key_size,im_value_size) emitMapCount(im_key_size,im_value_size,word_num,key_im_size,value_im_size,map_im_num,threadID)

/*extern */MAP_COUNT;


typedef struct MapFileList {
	char* filename;
	struct MapFileList* next;
}MapFileList;

typedef enum InputFormat{TextInputFormat,KeyValueInputFormat,SequenceFileInputFormat} input_format;

typedef struct Index{
	int key_offset;
	int key_size;
	int value_offset;
	int value_size;
}Index;

typedef struct MapReduceSpec{
	MapFileList* map_file_list;
	char* map_input_keys;
	char* map_input_values;
	Index* map_input_index;
	int* map_im_key_size;
	int* map_im_value_size;
	int* map_im_num;
	char* im_keys;
	int* im_values;
	Index* im_index;
	int map_input_num;
	int map_block_num;
	int map_thread_num;
	input_format map_input_format;
}MapReduceSpec;

char* d_map_input_keys;
char* d_map_input_values;
Index* d_map_input_index;

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

void init_mapreduce_spec(MapReduceSpec* spec){
	spec->map_file_list=NULL;
	spec->map_input_keys=NULL;
	spec->map_input_values=NULL;
	spec->map_input_index=NULL;
	spec->map_im_key_size=NULL;
	spec->map_im_value_size=NULL;
	spec->map_im_num=NULL;
	spec->im_keys=NULL;
	spec->im_values=NULL;
	spec->im_index=NULL;
	spec->map_input_num=0;
	spec->map_block_num=0;
	spec->map_thread_num=512;
	spec->map_input_format=TextInputFormat;
}

void free_spec(MapReduceSpec* spec){
	free_map_file_list(spec->map_file_list);
	free(spec->map_input_keys);
	free(spec->map_input_values);
	free(spec->map_input_index);
	free(spec->map_im_key_size);
	free(spec->map_im_value_size);
	free(spec->map_im_num);
	free(spec->im_keys);
	free(spec->im_values);
	free(spec->im_index);
	free(spec);
}

char *my_strncpy(char *dest, const char *src, size_t n)
{
    size_t i;

   for (i = 0; i < n && src[i] != '\0'; i++)
        dest[i] = src[i];
    for ( ; i < n; i++)
        dest[i] = '\0';

   return dest;
}

void map_input_split(MapReduceSpec* spec){
	MapFileList* file_list_entry;
	size_t buffer_size=(size_t)256*1024*1024;
	size_t buffer_used=0;

	FILE* pFile;
	file_list_entry=spec->map_file_list;	
	
	size_t file_size;
	size_t key_array_size;
	size_t value_array_size;
	size_t index_array_size;

	if(spec->map_input_format==TextInputFormat){
		file_size=key_array_size=value_array_size=index_array_size=0;

		while(file_list_entry->filename!=NULL){
			pFile=fopen(file_list_entry->filename,"rb");
			if (pFile==NULL) {fputs ("File error\n",stderr); exit (1);}
			fseek (pFile , 0 , SEEK_END);
			file_size = ftell (pFile);
			rewind (pFile);
			if(buffer_used+file_size<=buffer_size){
				ssize_t result=0;
				while (result!= -1) {					
					size_t value_size = 0;
					size_t key_size=0;
					char* temp_key=NULL;
					char* temp_value=NULL;

					temp_key=(char*)malloc(10);
					sprintf(temp_key,"%d",(int)ftell(pFile));
					key_size=strlen(temp_key)+1;                      //get the new key's size

					spec->map_input_keys=(char*)realloc(spec->map_input_keys,key_array_size+key_size);             //reallocate key_array, so that it can contain new keys
					my_strncpy((spec->map_input_keys)+key_array_size,temp_key,key_size);
					result=getline(&(temp_value), &value_size, pFile);
					value_size=strlen(temp_value)+1;
					spec->map_input_values=(char*)realloc(spec->map_input_values,value_array_size+value_size);           //reallocate value_size, so that it can contain new values
					strcpy((char*)(spec->map_input_values+value_array_size),temp_value);
					spec->map_input_index=(Index*)realloc(spec->map_input_index,(index_array_size+1)*sizeof(Index));            //reallocate index array, so that it can contain new <key,value> information

					spec->map_input_index[index_array_size].key_offset=key_array_size;
					spec->map_input_index[index_array_size].key_size=key_size;					
					spec->map_input_index[index_array_size].value_offset=value_array_size;
					spec->map_input_index[index_array_size].value_size=value_size;

					key_array_size+=key_size;
					value_array_size+=value_size;
					index_array_size++;
					free(temp_key); free(temp_value);
				}
				buffer_used=buffer_used+file_size;
			}
			else
				printf("Buffer full!!\n");
			file_list_entry=file_list_entry->next;
			fclose(pFile);
		}
		spec->map_input_num=index_array_size;
//		printf("Map Input entry number: %i, %u, %u, %u\n",spec->map_input_num,key_array_size,value_array_size,index_array_size*sizeof(Index));
		printf("Map Input entry number: %i\n",spec->map_input_num);
	}	

}

__device__ bool isChar(char c){
	if(((c<='z')&&(c>='a'))||((c<='Z')&&(c>='A')))
		return true;
	else
		return false;
}

__device__ void emitMapCount(int key_size, int value_size,int word_num,int*key_im_size_array,int*value_im_size_array,int*map_im_num,int threadID){
	*(key_im_size_array+threadID)=key_size;
	*(value_im_size_array+threadID)=value_size;
	*(map_im_num+threadID)=word_num;
}

__global__ void map_count_warp(char*keys,char*values,Index*index,int*map_im_key_size,int*map_im_value_size,int*map_im_num,int input_num){
	int i=blockDim.x*blockIdx.x+threadIdx.x;
	if(i<input_num){
		mapCount((keys+((index+i)->key_offset)),(values+((index+i)->value_offset)),(index+i)->key_size,(index+i)->value_size,map_im_key_size,map_im_value_size,map_im_num,i);
	}
}

void map_count_phase(MapReduceSpec* spec){
	// char* d_map_input_keys;
	// char* d_map_input_values;
	// Index* d_map_input_index;
	int* d_map_im_key_size;
	int* d_map_im_value_size;
	int* d_map_im_num;
	size_t map_im_size=(spec->map_input_num)*sizeof(int);
	spec->map_im_key_size=(int*)malloc(map_im_size);
	spec->map_im_value_size=(int*)malloc(map_im_size);
	spec->map_im_num=(int*)malloc(map_im_size);

	size_t keys_size=malloc_usable_size(spec->map_input_keys);
	size_t values_size=malloc_usable_size(spec->map_input_values);
	size_t index_size=malloc_usable_size(spec->map_input_index);
	//printf("%u,%u,%u\n",malloc_usable_size(spec->map_input_keys),malloc_usable_size(spec->map_input_values),malloc_usable_size(spec->map_input_index));
	cudaMalloc(&d_map_input_keys,keys_size);
	cudaMalloc(&d_map_input_values,values_size);
	cudaMalloc(&d_map_input_index,index_size);
	cudaMalloc(&d_map_im_key_size,map_im_size);
	cudaMalloc(&d_map_im_value_size,map_im_size);
	cudaMalloc(&d_map_im_num,map_im_size);
	cudaMemcpy(d_map_input_keys,spec->map_input_keys,keys_size,cudaMemcpyHostToDevice);
	cudaMemcpy(d_map_input_values,spec->map_input_values,values_size,cudaMemcpyHostToDevice);
	cudaMemcpy(d_map_input_index,spec->map_input_index,index_size,cudaMemcpyHostToDevice);
	spec->map_block_num=((spec->map_input_num)+(spec->map_thread_num)-1)/(spec->map_thread_num);
//	printf("%d\n",spec->map_block_num);
	map_count_warp<<<spec->map_block_num,spec->map_thread_num>>>(d_map_input_keys,d_map_input_values,d_map_input_index,d_map_im_key_size,d_map_im_value_size,d_map_im_num,spec->map_input_num);
	cudaMemcpy(spec->map_im_key_size,d_map_im_key_size,map_im_size,cudaMemcpyDeviceToHost);
	cudaMemcpy(spec->map_im_value_size,d_map_im_value_size,map_im_size,cudaMemcpyDeviceToHost);
	cudaMemcpy(spec->map_im_num,d_map_im_num,map_im_size,cudaMemcpyDeviceToHost);
//	printf("%s\n",spec->map_input_values);
//	printf("%d %d %d\n",*(spec->map_im_key_size),*(spec->map_im_value_size),*(spec->map_im_num));
	// cudaFree(d_map_input_keys);
	// cudaFree(d_map_input_values);
	// cudaFree(d_map_input_index);
	cudaFree(d_map_im_key_size);
	cudaFree(d_map_im_value_size);
	cudaFree(d_map_im_num);
}

__device__ void im_emit(char* key,int start, int end, int value,char* im_key,int* im_value,Index* im_index,int g_im_key_offset,int g_im_value_offset,int key_local_offset,int word_num){
	int i;
//	static __shared__ int j=0;
	// for(i=start;i<end;i++){
	// 	*(im_key+key_local_offset+i-start)=*(key+i);
	// 	//*(im_key+(*j))='d';
	// 	//(*j)++;
	// }
	*im_key='i';*(im_key+1)='p';
//	*im_key='t';*(im_key+1)='s';
	*(im_value+word_num)=value;
	(im_index+word_num)->key_offset=g_im_key_offset+start;
	(im_index+word_num)->key_size=end-start;
	(im_index+word_num)->value_offset=g_im_value_offset+word_num;
	(im_index+word_num)->value_size=1;
}

__device__ void map(char*key,char*value,size_t key_size, size_t value_size,char* im_key,int*im_value,int im_key_size,int im_value_size,Index* im_index,int g_im_key_offset,int g_im_value_offset,int g_im_index_offset){
	int i=0;
	int start;
	int local_offset=0;
	int key_local_offset=0;
	int word_num=0;
	while(i<value_size){
		while((i<value_size)&&!isChar(*(value+i)))
			i++;
		start = i;
		while((i<value_size)&&isChar(*(value+i)))
			i++;
		if(start<i){
			if(key_local_offset==0){
				im_emit(value,start,i,1,im_key,im_value,im_index+g_im_index_offset,g_im_key_offset,g_im_value_offset,key_local_offset,word_num);}

			//break;
		}
		key_local_offset=key_local_offset+(i-start);
		//key_local_offset++;
		word_num++;
	}
}

__global__ void map_warp(char*input_keys,char*input_values,Index*input_index,char*im_key,int*im_value,Index*im_index,Index*im_loc,int* im_index_loc,int input_num){
	int i=blockDim.x*blockIdx.x+threadIdx.x;
	if(i==0){
		map((input_keys+((input_index+i)->key_offset)),(input_values+((input_index+i)->value_offset)),(input_index+i)->key_size,(input_index+i)->value_size,(im_key+(im_loc+i)->key_offset),(im_value+(im_loc+i)->value_offset),(im_loc+i)->key_size,(im_loc+i)->value_size,im_index,(im_loc+i)->key_offset,(im_loc+i)->value_offset,*(im_index_loc+i));
	}
}

void map_phase(MapReduceSpec* spec){
	int im_key_total_size=0;
	int im_value_total_size=0;
	int im_num_total=0;
	Index im_loc[spec->map_input_num];
	int im_index_loc[spec->map_input_num];

	for(int i=0; i<spec->map_input_num;i++){
		im_loc[i].key_offset=im_key_total_size;
		im_loc[i].key_size=*(spec->map_im_key_size+i);
		im_loc[i].value_offset=im_value_total_size>>2;
		im_loc[i].value_size=1;
		im_index_loc[i]=im_num_total;
		im_key_total_size+=*(spec->map_im_key_size+i);
		im_value_total_size+=*(spec->map_im_key_size+i);
		im_num_total+=*(spec->map_im_num+i);
	}
	printf("Map outpu entries: %d\n",im_num_total);	

	spec->im_keys=(char*)malloc(im_key_total_size);
	spec->im_values=(int*)malloc(im_value_total_size);
	spec->im_index=(Index*)malloc(im_num_total*sizeof(Index));
	
	char *d_im_keys;
	int *d_im_values;
	Index *d_im_index;
	Index *d_im_loc;
	int *d_im_index_loc;
	cudaMalloc(&d_im_keys,im_key_total_size);
	cudaMalloc(&d_im_values,im_value_total_size);
	cudaMalloc(&d_im_index,im_num_total*sizeof(Index));
	cudaMalloc(&d_im_loc,spec->map_input_num*sizeof(Index));
	cudaMemcpy(d_im_loc,im_loc,spec->map_input_num*sizeof(Index),cudaMemcpyHostToDevice);
	cudaMalloc(&d_im_index_loc,spec->map_input_num*sizeof(int));
	cudaMemcpy(d_im_index_loc,im_index_loc,spec->map_input_num*sizeof(int),cudaMemcpyHostToDevice);
	map_warp<<<spec->map_block_num,spec->map_thread_num>>>(d_map_input_keys,d_map_input_values,d_map_input_index,d_im_keys,d_im_values,d_im_index,d_im_loc,d_im_index_loc,spec->map_input_num);
	cudaMemcpy(spec->im_keys,d_im_keys,im_key_total_size,cudaMemcpyDeviceToHost);
	cudaMemcpy(spec->im_values,d_im_values,im_value_total_size,cudaMemcpyDeviceToHost);
	cudaMemcpy(spec->im_index,d_im_index,im_num_total*sizeof(Index),cudaMemcpyDeviceToHost);	
	printf("%s\n%s\n",spec->im_keys,spec->map_input_values);
	
	free(spec->map_input_keys);
	free(spec->map_input_values);
	free(spec->map_input_index);
	free(spec->map_im_key_size);
	cudaFree(d_map_input_keys);
	cudaFree(d_map_input_values);
	cudaFree(d_map_input_index);
	cudaFree(d_im_keys);
	cudaFree(d_im_values);
	cudaFree(d_im_index);
	cudaFree(d_im_loc);
	cudaFree(d_im_index_loc);
}

void add_input_path(char *path,MapReduceSpec* spec){
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
	map_count_phase(spec);
	map_phase(spec);
}

MAP_COUNT{
	unsigned int i;
	unsigned int im_key_size=0;
	unsigned int im_value_size=0;
	int word_num=0;
	for(i=0;i<value_size;){
		while((i<value_size)&&!isChar(*(value+i)))
			i++;
		int start = i;
		while((i<value_size)&&isChar(*(value+i)))
			i++;
		if(start<i){
			im_key_size+=(i-start);
			im_value_size+=sizeof(int);
			word_num++;
		}
	}
	EMIT_IM_COUNT(im_key_size,im_value_size);
	//emitMapCount(im_key_size,im_value_size,word_num,key_im_size,value_im_size,map_im_num,threadID);
}

int main(int argc, char **argv){
	MapReduceSpec* spec=(MapReduceSpec*)malloc(sizeof(MapReduceSpec));
	init_mapreduce_spec(spec);
	add_input_path(argv[1],spec);
	free(spec);
}

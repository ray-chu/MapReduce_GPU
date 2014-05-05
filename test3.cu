#include "mrLib.cu"

__device__ void MAP_COUNT{
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

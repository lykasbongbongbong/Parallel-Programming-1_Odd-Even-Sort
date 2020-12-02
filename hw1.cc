#include <cstdio>
#include <mpi.h>
#include <math.h>
#include <stdlib.h>
#include <algorithm>
#include <time.h>

int merge(float *data, int data_length, float *remote, int remote_length, float *all){
    int i, j=0;
    for (int i = 0; i < data_length; i++){
        all[i] = data[i];
        j++;
    }
    for (int i=0; i < remote_length; i++){
        all[j] = remote[i]; 
        j++;
    }
    std::sort(all, all+data_length+remote_length);
    return 0;
}
void MPI_Odd_Even_Sort(int size, int localn, int mod, float *data, int sendrank, int recvrank, MPI_Comm comm){
    int rank;
    float *remote = (float*) malloc(sizeof(float)*localn);
    float *all = (float*) malloc(sizeof(float)*(localn*2+mod));
    const int mergetag = 1;
    const int sortedtag = 2;
    MPI_Comm_rank(comm, &rank);
    //printf("go!\n");
    //如果現在的rank是送的rank 那他就要負責送跟收到時候已經排序好的


    
    if(rank==sendrank){
        
        MPI_Send(data, localn, MPI_FLOAT, recvrank, mergetag, MPI_COMM_WORLD);
        MPI_Recv(data, localn, MPI_FLOAT, recvrank, sortedtag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
    }
    else{
        
        MPI_Recv(remote, localn, MPI_FLOAT, sendrank, mergetag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if(rank!=size-1)
            merge(data, localn, remote, localn, all);
        else
            merge(data, localn+mod, remote, localn, all);
        MPI_Send(&(all[0]), localn, MPI_FLOAT, sendrank, sortedtag, MPI_COMM_WORLD);
        
        if(rank==size-1)
            for(int i=localn; i<localn*2+mod;i++)
                data[i-localn] = all[i];
        else{
            for(int i=localn; i<localn*2;i++)
                data[i-localn] = all[i];
        }
        
    }
    

    

}
int main(int argc, char** argv) {
    
    MPI_Init(&argc,&argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Status status;
    MPI_File f, out;
    int n = atoi(argv[1]);

   
    MPI_File_open(MPI_COMM_WORLD, argv[2], MPI_MODE_RDONLY, MPI_INFO_NULL, &f);
    MPI_File_open(MPI_COMM_WORLD, argv[3], MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &out);
     
    //all_time = tt2.tv_nsec - tt1.tv_nsec;
    //printf("consumes %ld ms!\n", (tt2.tv_nsec - tt1.tv_nsec)/1000000);

    int partition = n/size;
    int mod = n%size;
    if(rank==size-1)
        partition+=mod;
    float *data = (float*) malloc(sizeof(float)*partition);
    if(rank==size-1)
        partition-=mod;
 
    //read file
    if(rank!=size-1)
        MPI_File_read_at(f, sizeof(float)*partition*rank, data, partition, MPI_FLOAT, MPI_STATUS_IGNORE);
    else
        MPI_File_read_at(f, sizeof(float)*partition*rank, data, partition+mod, MPI_FLOAT, MPI_STATUS_IGNORE);
    

    //struct timespec tt1, tt2;
    //clock_gettime(CLOCK_REALTIME, &tt1);
    //local先做qsort
    
    if(rank!=size-1)
        std::sort(data, data+partition);
    else
        std::sort(data, data+partition+mod);
    
    int i;
    //odd-even sort
    for(i=0;i<=size;i++){
        if((i+rank)%2==0){//phase和rank同質 同為奇數 同為偶數 phase奇數 就奇數做 phase偶數就偶數做
            if(rank<size-1){
                MPI_Odd_Even_Sort(size, partition, mod, data, rank, rank+1, MPI_COMM_WORLD);
            }
        }
        else if(rank>0){ //和當前phase不同質
            MPI_Odd_Even_Sort(size, partition, mod, data, rank-1, rank, MPI_COMM_WORLD);
        }

    }
     

    //clock_gettime(CLOCK_REALTIME, &tt2); 
    //printf("consumes %ld ms!\n", (tt2.tv_nsec - tt1.tv_nsec)/1000000);
    //一個一個rank寫入檔案
    
    
    if(rank!=size-1){
        MPI_File_write_at(out, sizeof(float)*partition*rank, data, partition, MPI_FLOAT, MPI_STATUS_IGNORE);
    }
    else{
        MPI_File_write_at(out, sizeof(float)*partition*rank, data, partition+mod, MPI_FLOAT, MPI_STATUS_IGNORE);
    }
    


    //檢查有沒有錯
    /*
    float *result = (float*) malloc(sizeof(float)*n);
    MPI_Gather(data, n / size, MPI_FLOAT, result, n / size, MPI_FLOAT, 0, MPI_COMM_WORLD);
    if (rank == 0){
        for (int j=0; j<n-1; j++){
            printf("%f ",result[j]);
        }
        printf("%f\n", result[n-1]);

    }*/

    
    MPI_File_close(&f);
    MPI_File_close(&out);
    return MPI_SUCCESS;
   
   
    MPI_Finalize();
    

}


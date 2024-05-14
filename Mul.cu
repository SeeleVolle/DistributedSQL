__global__ void matrix(float *MatrixA, float *matrixB, float *matrixC){
    int nRow = blockIdx.y * blockDim.y+  threadIdx.y;
    int nCol = blockIdx.x * blockDim.x + threadIdx.x;
    float Cval = 0.0;

    for(int i = 0; i < k; i++)
    {
        fCval += MatrixA[nRow*K+i] * MatrixB[i*n+nCol];
    }
    matrixC[nRow*n+nCol] = fCval;
}
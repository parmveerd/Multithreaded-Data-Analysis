#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <math.h>
#include <ctype.h>
#include <stdatomic.h>

#define MAX_FILES 64

pthread_mutex_t mutex;
pthread_cond_t condition;
pthread_mutex_t* entryLocks;

int flag = 0;
int currentThread = 0;

typedef struct {
    char* filenames[MAX_FILES];
    int fileCount;
    int fileIndex;
    int number;
} ThreadData;

ThreadData threadData[64];

typedef struct {
    char filename[64];
    float calculated[1024];
    int lineCount;
    int finished;
    int leftoff;
    float alpha;
    float beta;
} FileInfo;

FileInfo fileData[256];
int fileDataCount = 0;

int longestLine = 0;
int thread_num = 0;
int buffer_size = 3;
int avg;
int lock_config = 1;
int file_count = 0;
int files = 0;
float values[MAX_FILES*2];

float lowpass(float sample, float alpha, float prev) {
    return alpha*sample + (1-alpha)*prev;    
}

float amplification(float sample, float beta) {
    return beta*sample;
}

void help2(int index) {
    int index2;

    index2 = index;

    while (1) {
        if (index2 >= files) {
            return;
        }
        if (fileData[index2].finished == 0) {
            break;
        }
        index2 += thread_num;
    }

    int in = index2;
    int ind = 0;
    char input[50] = {'\0'};
    char temp_input;
    int input_index = 0;
    int buffer_count = 0;
    int loopindex = 0;
    int loopcheck = 1;
    int check = 0;
    int check2 = 0;
    float a = 0.0;
    int l;

    FILE* fp = fopen(fileData[in].filename, "r");

    while ((temp_input = fgetc(fp)) != EOF) {
        if (fileData[in].leftoff > loopindex) {
            loopindex++;
            continue;
        }

        if (isdigit(temp_input)) {
            input[input_index++] = temp_input;
        }

        if (temp_input != '\r') {
            buffer_count++;
        } else {
            loopindex++;
            continue;
        }

        if (buffer_count == buffer_size) {
            l = fileData[in].lineCount;
            if (l == 0) {
                a = atoi(input);
            } else {
                a = lowpass(atoi(input), fileData[in].alpha, fileData[in].calculated[l-1]);
            }
            fileData[in].calculated[fileData[in].lineCount++] = a;
            memset(input, 0, sizeof(input));
            buffer_count = 0;
            loopindex++;
            check2 = 1;
            break;
        }
        loopindex++;
    }

    // If finishes early
    if (input[0] != '\0') {
        if (buffer_count > 0) {
            l = fileData[in].lineCount;
            if (l == 0) {
                a = atoi(input);
            } else {
                a = lowpass(atoi(input), fileData[in].alpha, fileData[in].calculated[l-1]);
            }
            fileData[in].calculated[fileData[in].lineCount++] = a;
            fileData[in].finished = 1;
            memset(input, 0, sizeof(input));
            buffer_count = 0;
        }
    }
    fileData[in].leftoff = loopindex;
    fclose(fp);
    if (check2 == 0) {
        fileData[in].finished = 1;
        // Re-call the function with same thread if other file finished
        help2(index);
    }    
}


void help(int index) {
    char* filenames[avg];
    for (int i = 0; i < avg; i++) {
        filenames[i] = threadData[index].filenames[i];
    }
    int start = file_count;
    int count = index;
    for (int i = 0; i < avg; i++) {
        strcpy(fileData[file_count].filename, filenames[i]);
        fileData[file_count].finished = 0;
        fileData[file_count].leftoff = 0;
        fileData[file_count].alpha = values[count*2];
        fileData[file_count].beta = values[count*2+1];
        file_count++;
        count += thread_num;
    }
    
    int in = start;
    int ind = 0;
    char input[50] = {'\0'};
    char temp_input;
    int input_index = 0;
    int buffer_count = 0;
    int loopindex = 0;
    int loopcheck = 1;
    int loop_count = 0;
    int check = 0;
    int check2 = 0;
    float a = 0.0;
    int l;

    while (loopcheck && loop_count < 1000) {
        loop_count++;
        if (in >= file_count) {
            in = start;
        }
        FILE* fp = fopen(fileData[in].filename, "r");
        loopindex = 0;

        while ((temp_input = fgetc(fp)) != EOF) {
            if (fileData[in].leftoff > loopindex) {
                loopindex++;
                continue;
            }

            if (isdigit(temp_input)) {
                input[input_index++] = temp_input;
            }

            // Do not count \r as a character
            if (temp_input != '\r') {
                buffer_count++;
            } else {
                loopindex++;
                continue;
            }

            if (buffer_count == buffer_size) {
                l = fileData[in].lineCount;
                if (l == 0) {
                    a = atoi(input);
                } else {
                    a = lowpass(atoi(input), fileData[in].alpha, fileData[in].calculated[l-1]);
                }
                fileData[in].calculated[fileData[in].lineCount++] = a;
                memset(input, 0, sizeof(input));
                buffer_count = 0;
                loopindex++;
                check2 = 1;
                break;
            }
            loopindex++;
        }

        // If finishes early
        if (input[0] != '\0') {
            if (buffer_count > 0) {
                l = fileData[in].lineCount;
                if (l == 0) {
                    a = atoi(input);
                } else {
                    a = lowpass(atoi(input), fileData[in].alpha, fileData[in].calculated[l-1]);
                }
                if (fileData[in].finished == 0) {
                    fileData[in].calculated[fileData[in].lineCount++] = a;
                }
                fileData[in].finished = 1;
                memset(input, 0, sizeof(input));
                buffer_count = 0;
            }
        }
        fileData[in].leftoff = loopindex;
        fclose(fp);
        if (check2 == 0) {
            fileData[in].finished = 1;
        }

        check2 = 0;
        input_index = 0;
        memset(input, 0, sizeof(input));
        buffer_count = 0;
        in++;
        ind++;
        for (int i = start; i < file_count; i++) {
            if (fileData[i].finished == 0) {
                check = 1;
            }
        }

        if (check == 0) {
            loopcheck = 0;
            break;
        }
        check = 0;
    }

    if (lock_config != 2) {
        for (int i = start; i < file_count; i++) {
            l = fileData[i].lineCount;
            for (int j = 0; j < l; j++) {
                fileData[i].calculated[j] = amplification(fileData[i].calculated[j], fileData[i].beta);
            }
        }
    }
}

void* threadFunc(void* arg) {
    ThreadData* data = (ThreadData*)arg;
    int a = data->number;

    if (lock_config == 2) {
        pthread_mutex_lock(&entryLocks[a]);
    } else {
        pthread_mutex_lock(&mutex);
    }

    help(a);

    if (lock_config == 2) {
        pthread_mutex_unlock(&entryLocks[a]);
    } else {
        pthread_mutex_unlock(&mutex);
    }

    pthread_exit(NULL);
}

void* threadFunc2(void* arg) {
    ThreadData* data = (ThreadData*)arg;
    int threadID = data->number;

    int loopCount = 0;
    while (loopCount < 1000) {
        loopCount++;
        if (lock_config == 2) {
            pthread_mutex_lock(&entryLocks[threadID]);
        } else {
            pthread_mutex_lock(&mutex);
        }
        while (threadID != currentThread) {
            if (lock_config != 2) {
                pthread_cond_wait(&condition, &mutex);
            }
        }

        help2(threadID);

        currentThread = (currentThread+1) % thread_num;
        int done = 1;
        for (int i = 0; i < files; i++) {
            if (fileData[i].finished == 0) {
                done = 0;
                break;
            }
        }
        if (done == 1) {
            if (lock_config == 2) {
                pthread_mutex_unlock(&entryLocks[threadID]);
            } else {
                pthread_cond_broadcast(&condition);
                pthread_mutex_unlock(&mutex);
            }
            break;
        }

        if (lock_config == 2) {
            pthread_mutex_unlock(&entryLocks[threadID]);
        } else {
            pthread_cond_broadcast(&condition);
            pthread_mutex_unlock(&mutex);
        }
    }
    pthread_exit(NULL);
}


int main(int argc, char* argv[]) {
    if (argc != 7) {
        printf("ERROR: Make sure there are 6 arguments. There were %d argument(s) instead.\n", argc-1);
        return 1;
    }

    FILE* fptr = fopen(argv[3], "r");
    if (fptr == NULL) {
        printf("ERROR: Cannot open metadata file.");
        return 1;
    }

    int line_count = 0;
    char line[64] = {'\0'};
    int counter = 0;
    int store = 3;
    char* filenames[MAX_FILES];
    for (int i = 0; i < MAX_FILES; i++) {
        filenames[i] = malloc(64+1);
    }
    int fileI = 0;
    int failure = 0;

    int filee;
    fscanf(fptr, "%d\n", &filee);

    while (fgets(line, sizeof(line), fptr) != NULL) {
        if (line_count = 0) {
        } else {
            int checker = 0;
            int string = 0;
            while (line[checker] != ' ' && line[checker] != '\n' && checker < sizeof(line)) {
                if (isalpha(line[checker])) {
                    string = 1;
                    break;
                }
                checker++;
            }
            if (string == 1) {
                if (store == 0) {
                    values[counter++] = 1.0;
                    values[counter++] = 1.0;
                } else if (store == 1) {
                    values[counter++] = 1.0;
                }
                store = 0;
                if (line[strlen(line)-1] == '\n') {
                    line[strlen(line)-1] = '\0';
                }
                strcpy(filenames[fileI++], line);
                string = 0;
            } else {
                store++;
                if (store == 3) {
                    printf("ERROR: The metadata file is incorrect. Please make sure the file does not contain 3 or more consecutive lines without a filename.\n");
                    for (int e = 0; e < MAX_FILES; e++) {
                        free(filenames[e]);
                    }
                    fclose(fptr);
                    return 1;
                }
                if (line[strlen(line)-1] == '\n') {
                    line[strlen(line)-1] = '\0';
                }
                values[counter++] = atof(line);
            }
        }
        line_count++;
        memset(line, 0, sizeof(line));
    }

    fclose(fptr);

    if (store == 0) {
        values[counter++] = 1.0;
        values[counter++] = 1.0;
    } else if (store == 1) {
        values[counter++] = 1.0;
    }


    files = filee;

    for (int i = 0; i < files; i++) {
        if (values[i*2] < 0.0 || values[i*2] > 1.0) {
            values[i*2] = 1.0;
        }
        if (values[i*2+1] <= 0.0) {
            values[i*2+1] = 1.0;
        }
    }


    thread_num = atoi(argv[2]);
    lock_config = atoi(argv[4]);

    buffer_size = atoi(argv[1]);
    int isNum = 1;
    for (int i = 0; argv[5][i] != '\0'; i++) {
        if (!isdigit(argv[5][i])) {
            isNum = 0;
            break;
        }
    }

    if (thread_num == 0 || lock_config == 0 || buffer_size == 0 || isNum == 0) {
        printf("ERROR: Please make sure number of threads, lock config, and buffer size are not 0. Also make sure the arguments are numbers.\n");
        for (int e = 0; e < MAX_FILES; e++) {
            free(filenames[e]);
        }
        return 1;
    }

    int global_checkpoint = atoi(argv[5]);

    if (thread_num == 1) {
        global_checkpoint = 0;
    }

    if (files % thread_num != 0) {
        printf("ERROR: Files are not a multiple of threads.\n");
        for (int e = 0; e < MAX_FILES; e++) {
            free(filenames[e]);
        }
        return 1;
    }

    avg = files/thread_num;


    for (int i = 0; i < thread_num; i++) {
        int fileIndex = i;

        for (int j = 0; j < avg; j++) {
            int fileNumber = fileIndex + (j * thread_num);
            threadData[i].filenames[j] = filenames[fileNumber];
        }
        threadData[i].fileCount = avg;
        threadData[i].fileIndex = fileIndex;
        threadData[i].number = i;
    }


    for (int i = 0; i < files; i++) {
        strcpy(fileData[i].filename, filenames[i]);
        fileData[i].finished = 0;
        fileData[i].leftoff = 0;
        fileData[i].lineCount = 0;
        fileData[i].alpha = values[i*2];
        fileData[i].beta = values[i*2+1];
    }

    printf("Calculating...\n");

    if (lock_config == 2) {
        entryLocks = malloc(thread_num * sizeof(pthread_mutex_t));
        for (int i = 0; i < thread_num; i++) {
            pthread_mutex_init(&entryLocks[i], NULL);
        }
    } else {
        pthread_mutex_init(&mutex, NULL);
    }

    pthread_cond_init(&condition, NULL);

    
    pthread_t threads[thread_num];
    for (int i = 0; i < thread_num; ++i) {
        if (global_checkpoint == 1) {
            pthread_create(&threads[i], NULL, threadFunc2, &threadData[i]);
        } else {
            pthread_create(&threads[i], NULL, threadFunc, &threadData[i]);
        }
    }

    for (int i = 0; i < thread_num; ++i) {
        pthread_join(threads[i], NULL);
    }

    if (lock_config == 2) {
        for (int i = 0; i < thread_num; i++) {
            pthread_mutex_destroy(&entryLocks[i]);
        }
        free(entryLocks);
    } else {
        pthread_mutex_destroy(&mutex);
    }
    pthread_cond_destroy(&condition);


    FILE* fp = fopen(argv[6], "w");
    float sum;
    unsigned int sum2;
    int longest = 0;
    for (int i = 0; i < files; i++) {
        if (fileData[i].lineCount > longest) {
            longest = fileData[i].lineCount;
        }
    }

    if (global_checkpoint == 1 || lock_config == 2) {
        for (int i = 0; i < files; i++) {
            for (int j = 0; j < longest; j++) {
                fileData[i].calculated[j] = fileData[i].beta * fileData[i].calculated[j];
            }
        }
    }
    for (int i = 0; i < longest; i++) {
        sum = 0;
        for (int j = 0; j < files; j++) {
            sum += fileData[j].calculated[i];
        }
        if (sum > 65535) {
            sum2 = 65535;
        } else {
            sum2 = ceil(sum);
        }
        fprintf(fp, "%d\n", sum2);
        sum = 0;
    }

    fclose(fp);

    for (int i = 0; i < MAX_FILES; i++) {
        free(filenames[i]);
    }

    printf("Done!\n");

    return 0;
}
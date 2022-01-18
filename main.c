#include <mpi.h>
#include <stdio.h>
#include <math.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

#define READER 0
#define MAPPER_1 1
#define MAPPER_2 2
#define MAPPER_3 3
#define REDUCER_1 4
#define REDUCER_2 5

int max(int a, int b) {
    return a >= b ? a : b;
}

int is_vowel(char c) {
    return c == 'a' || c == 'A'
        || c == 'e' || c == 'E'
        || c == 'i' || c == 'I'
        || c == 'o' || c == 'O'
        || c == 'u' || c == 'U';
}

char *trimString(char *str) {
    char *end;

    while(isspace((unsigned char)*str)) str++;

    if(*str == 0)
        return str;

    end = str + strlen(str) - 1;
    while(end > str && isspace((unsigned char)*end)) end--;

    end[1] = '\0';

    return str;
}

int main(int argc, char * argv[]) {
    int rank, nProcesses;

    MPI_Init(&argc, &argv);
	MPI_Status status;
	MPI_Request request;

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nProcesses);

    if (nProcesses != 6) {
		printf("please run with: mpirun --oversubscribe -np 6 %s\n", argv[0]);
		MPI_Finalize();	
		exit(0);
	}

    if (rank == READER) {
        FILE* fp;
        fp = fopen(argv[1], "r");

        if (fp == NULL) {
            perror("Failed to open file\n");
            MPI_Finalize();	
		    exit(0);
        }

        char first_line[10];
        char **lines;

        int no_lines;
        fscanf(fp, "%d\n", &no_lines);
        lines = malloc(no_lines * sizeof(char*));

        for (int i = 0; i < no_lines; i++) {
            lines[i] = malloc(15 * sizeof(char));
            fgets(lines[i], 15, fp);
            lines[i] = trimString(lines[i]);
        }
        fclose(fp);

        int chunk_size = (int) ceil(no_lines / 3);

        int first_index = 0;
        int second_index = first_index + chunk_size;
        int third_index = second_index + chunk_size;

        int first_chunk_size = second_index - first_index;
        int second_chunk_size = third_index - second_index;
        int third_chunk_size = no_lines - third_index;

        MPI_Send(&first_chunk_size, 1, MPI_INT, MAPPER_1, 0, MPI_COMM_WORLD);
        MPI_Send(&second_chunk_size, 1, MPI_INT, MAPPER_2, 0, MPI_COMM_WORLD);
        MPI_Send(&third_chunk_size, 1, MPI_INT, MAPPER_3, 0, MPI_COMM_WORLD);

        for (int i = first_index; i < second_index; i++) {
            int len = strlen(lines[i]);
            MPI_Send(&len, 1, MPI_INT, MAPPER_1, 0, MPI_COMM_WORLD);
            MPI_Send(lines[i], len, MPI_CHAR, MAPPER_1, 0, MPI_COMM_WORLD);
        }

        for (int i = second_index; i < third_index; i++) {
            int len = strlen(lines[i]);
            MPI_Send(&len, 1, MPI_INT, MAPPER_2, 0, MPI_COMM_WORLD);
            MPI_Send(lines[i], len, MPI_CHAR, MAPPER_2, 0, MPI_COMM_WORLD);
        }

        for (int i = third_index; i < no_lines; i++) {
            int len = strlen(lines[i]);
            MPI_Send(&len, 1, MPI_INT, MAPPER_3, 0, MPI_COMM_WORLD);
            MPI_Send(lines[i], len, MPI_CHAR, MAPPER_3, 0, MPI_COMM_WORLD);
        }

        for (int i = 0; i < no_lines; i++) {
            free(lines[i]);
        }
        free(lines);
    } else if (rank == MAPPER_1) {
        int chunk_size;
        MPI_Recv(&chunk_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        int vowels = 0;
        int consonants = 0;

        char** lines;
        lines = malloc(chunk_size * sizeof(char *));

        for (int i = 0; i < chunk_size; i++) {
            int len;
            lines[i] = malloc(15 * sizeof(char));
            MPI_Recv(&len, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(lines[i], len, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }

        for (int i = 0; i < chunk_size; i++) {
            printf("[%d] %s\n", rank, lines[i]);
            for (int j = 0; j < strlen(lines[i]); j++) {
                if (is_vowel(lines[i][j])) {
                    vowels++;
                } else {
                    consonants++;
                }
            }
        }

        printf("[%d] v=%d c=%d\n", rank, vowels, consonants);

        MPI_Send(&vowels, 1, MPI_INT, REDUCER_1, 0, MPI_COMM_WORLD);
        MPI_Send(&consonants, 1, MPI_INT, REDUCER_2, 0, MPI_COMM_WORLD);

        for (int i = 0; i < chunk_size; i++) {
            free(lines[i]);
        }
        free(lines);
    } else if (rank == MAPPER_2) {
        int chunk_size;
        MPI_Recv(&chunk_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        int vowels = 0;
        int consonants = 0;

        char** lines;
        lines = malloc(chunk_size * sizeof(char *));

        for (int i = 0; i < chunk_size; i++) {
            int len;
            lines[i] = malloc(15 * sizeof(char));
            MPI_Recv(&len, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(lines[i], len, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }

        for (int i = 0; i < chunk_size; i++) {
            printf("[%d] %s\n", rank, lines[i]);
            for (int j = 0; j < strlen(lines[i]); j++) {
                if (is_vowel(lines[i][j])) {
                    vowels++;
                } else {
                    consonants++;
                }
            }
        }

        printf("[%d] v=%d c=%d\n", rank, vowels, consonants);

        MPI_Send(&vowels, 1, MPI_INT, REDUCER_1, 0, MPI_COMM_WORLD);
        MPI_Send(&consonants, 1, MPI_INT, REDUCER_2, 0, MPI_COMM_WORLD);

        for (int i = 0; i < chunk_size; i++) {
            free(lines[i]);
        }
        free(lines);
    } else if (rank == MAPPER_3) {
        int chunk_size;
        MPI_Recv(&chunk_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        char** lines;
        lines = malloc(chunk_size * sizeof(char *));

        int vowels = 0;
        int consonants = 0;

        for (int i = 0; i < chunk_size; i++) {
            int len;
            lines[i] = malloc(15 * sizeof(char));
            MPI_Recv(&len, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(lines[i], len, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }

        for (int i = 0; i < chunk_size; i++) {
            printf("[%d] %s\n", rank, lines[i]);
            for (int j = 0; j < strlen(lines[i]); j++) {
                if (is_vowel(lines[i][j])) {
                    vowels++;
                } else {
                    consonants++;
                }
            }
        }

        printf("[%d] v=%d c=%d\n", rank, vowels, consonants);
        
        MPI_Send(&vowels, 1, MPI_INT, REDUCER_1, 0, MPI_COMM_WORLD);
        MPI_Send(&consonants, 1, MPI_INT, REDUCER_2, 0, MPI_COMM_WORLD);

        for (int i = 0; i < chunk_size; i++) {
            free(lines[i]);
        }
        free(lines);
    } else if (rank == REDUCER_1) {
        int v1, v2, v3;
        MPI_Recv(&v1, 1, MPI_INT, MAPPER_1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&v2, 1, MPI_INT, MAPPER_2, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&v3, 1, MPI_INT, MAPPER_3, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        printf("[%d] Vowels: %d\n", rank, (v1 + v2 + v3));
    } else if (rank == REDUCER_2) {
        int c1, c2, c3;
        MPI_Recv(&c1, 1, MPI_INT, MAPPER_1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&c2, 1, MPI_INT, MAPPER_2, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&c3, 1, MPI_INT, MAPPER_3, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        printf("[%d] Consonants: %d\n", rank, (c1 + c2 + c3));
    }

    MPI_Finalize();

    return 0;
}
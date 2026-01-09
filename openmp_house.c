#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <limits.h>
#include <unistd.h>  

#define MAX_LINE 1024
#define MAX_ENTRIES 10000

int is_integer(const char *str) {       // Function to check if a string is a valid integer
    if (!str || *str == '\0') return 0;
    while (*str) {
        if (*str < '0' || *str > '9') return 0;
        str++;
    }
    return 1;
}

void trim(char *str) {                  // Function to trim spaces and single quotes
    char *start = str;
    while (*start == ' ' || *start == '\t' || *start == '\'') start++;
    char *end = str + strlen(str) - 1;
    while (end > start && (*end == ' ' || *end == '\t' || *end == '\'')) end--;
    *(end + 1) = '\0';
    if (start != str) memmove(str, start, strlen(start) + 1);
}
int main() {
    char cwd[1024];
    if (getcwd(cwd, sizeof(cwd))) {
        printf("Current working directory: %s\n", cwd);
    }

    FILE *file = fopen("houses_new.txt", "r");
    if (!file) {
        perror("Unable to open houses.txt");
        return 1;
    }
    char line[MAX_LINE];
    int property_sizes[MAX_ENTRIES];
    int property_prices[MAX_ENTRIES];
    int size_count = 0, price_count = 0;

    int line_num = 0;
    while (fgets(line, sizeof(line), file)) {
        line_num++;
        if (line_num == 1) continue;

        char *token;
        int col = 0;

        line[strcspn(line, "\n")] = 0;  // Remove newline

        token = strtok(line, ",");
        while (token) {
            trim(token); 

            if (col == 4) { // Property Size (5th column)
                if (is_integer(token))
                    property_sizes[size_count++] = atoi(token);
            }
            if (col == 13) { // Price (14th column)
                if (is_integer(token))
                    property_prices[price_count++] = atoi(token);
            }

            token = strtok(NULL, ",");
            col++;
        }
    }
    fclose(file);
    //IF no data
    int count = (size_count < price_count) ? size_count : price_count;
    if (count == 0) {
        printf("No valid data found.\n");
        return 1;
    }
    int max_size = 0;
    int min_price = INT_MAX;

    double start = omp_get_wtime();
    #pragma omp parallel
    {
        int local_max = 0;
        int local_min = INT_MAX;

        #pragma omp for
        for (int i = 0; i < count; i++) {
            if (property_sizes[i] > local_max) local_max = property_sizes[i];
            if (property_prices[i] < local_min) local_min = property_prices[i];
        }

        #pragma omp critical
        {
            if (local_max > max_size) max_size = local_max;
            if (local_min < min_price) min_price = local_min;
        }
    }
    double end = omp_get_wtime();
    printf("\nLargest Property Size : %d\n", max_size);
    printf("Cheapest Property Price : %d\n", min_price);
    printf("Execution Time : %f seconds\n", end - start);

    return 0;
}
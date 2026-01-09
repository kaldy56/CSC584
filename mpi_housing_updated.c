#include <mpi.h>      // MPI functions
#include <stdio.h>    // I/O functions
#include <stdlib.h>   // atof(), malloc(), free()
#include <string.h>   // string manipulation

#define MAX_LINE 2048          // Max CSV line length
#define PROPERTY_SIZE_COL 4    // Column index for property size
#define PRICE_COL 13           // Column index for price

int main(int argc, char *argv[]) {

    MPI_Init(&argc, &argv);   // Initialize MPI

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); // Process rank
    MPI_Comm_size(MPI_COMM_WORLD, &size); // Total processes

    if (argc < 2) {  // Check input file
        if (rank == 0)
            printf("Usage: mpiexec -n <num_processes> %s <datafile>\n", argv[0]);
        MPI_Finalize();
        return 1;
    }

    double start_time = MPI_Wtime(); // Start timer

    FILE *file = fopen(argv[1], "r"); // Open dataset
    if (!file) {
        printf("Rank %d: Unable to open file.\n", rank);
        MPI_Finalize();
        return 1;
    }

    char line[MAX_LINE];

    // Detect and skip header if present
    long file_pos = ftell(file);
    if (fgets(line, MAX_LINE, file)) {
        if (line[0] < '0' || line[0] > '9')
            ; // header skipped
        else
            fseek(file, file_pos, SEEK_SET); // no header
    }

    double local_max_property_size = -1e9;
    double local_min_price = 1e18;
    int row = 0;

    // Read file line by line
    while (fgets(line, MAX_LINE, file)) {

        // Round-robin distribution
        if (row % size == rank) {
            double property_size = -1.0;
            double price = -1.0;

            char *ptr = line;
            char *next;
            int col = 0;

            // Parse CSV manually
            while (ptr && col <= PRICE_COL) {
                next = strchr(ptr, ',');
                if (next) *next = '\0';

                while (*ptr == ' ' || *ptr == '\t') ptr++; // trim leading
                char *end = ptr + strlen(ptr) - 1;
                while (end > ptr && (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r')) *end-- = '\0'; // trim trailing

                if (col == PROPERTY_SIZE_COL) property_size = atof(ptr);
                if (col == PRICE_COL) price = atof(ptr);

                col++;
                ptr = next ? next + 1 : NULL;
            }

            if (property_size > local_max_property_size) local_max_property_size = property_size;
            if (price > 0 && price < local_min_price) local_min_price = price;
        }

        row++;
    }

    fclose(file); // Close file

    double global_max_property_size = 0.0;
    double global_min_price = 0.0;

    // Reduce local results to global
    MPI_Reduce(&local_max_property_size, &global_max_property_size, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_min_price, &global_min_price, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);

    // Measure execution time
    double end_time = MPI_Wtime();
    double local_elapsed = end_time - start_time;
    double max_elapsed;
    MPI_Reduce(&local_elapsed, &max_elapsed, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    // Print results from root
    if (rank == 0) {
        printf("=====================================\n");
        printf("Largest Property Size   : %.2f\n", global_max_property_size);
        printf("Cheapest Property Price : %.2f\n", global_min_price);
        printf("Execution Time          : %.6f seconds\n", max_elapsed);
        printf("=====================================\n");
    }

    MPI_Finalize(); // Finalize MPI
    return 0;
}
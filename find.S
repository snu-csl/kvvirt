.global find

.type find, @function

find:
    # Load the search value into a YMM register
    vpbroadcastd %esi, %ymm0

    # Initialize the index to -1 (not found)
    movq $-1, %rax

    mov %rdx, %r9 # Move total elements count into %r9

    # Loop through the table array in 8-element chunks
    xor %rcx, %rcx # Initialize loop counter
    mov %rdi, %r8 # Save the base address of the table

.loop:
    # Load 4 32-bit integers into two YMM registers
    vpmovzxdq (%r8, %rcx, 4), %ymm2
    vpmovzxdq 16(%r8, %rcx, 4), %ymm3

    //# Merge the two YMM registers into one containing 8 32-bit integers
    vpunpckldq %ymm3, %ymm2, %ymm4

    # Compare the loaded values with the search value
    vpcmpeqd %ymm0, %ymm4, %ymm3

    # Check if any of the comparisons were true
    vpmovmskb %ymm3, %r10
    test %r10, %r10
    jz .next_chunk

    # Find the position of the first set bit in r10-
    bsf %r10, %r10

    # Calculate the overall index
    shr $3, %r10
    shr $1, %rcx

    add %r10, %rcx
    //mov %r10, %rcx # Add offset within the chunk
    mov %rcx, %rax # Store the overall index in %rax

    jmp .exit

.next_chunk:
    # Increment the loop counter and check if we've reached the end
    add $8, %rcx
    cmp %r9, %rcx
    movq $-100, %rax
    jne .exit
    //jne .loop

.exit:
    ret

.size find, .-find


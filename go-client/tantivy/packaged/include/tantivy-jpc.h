#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * # Safety
 *
 */
uint8_t init(void);

/**
 * # Safety
 *
 */
int8_t term(const char *s);

int8_t set_k_and_b(float k, float b);

/**
 * tantivy_jpc is the main entry point into a translation layer from Rust to Go for Tantivy this function will # Steps   * parse the input for the appropriately formatted json   * Modify internal state to reflect json requests
 * # Safety
 *
 */
int64_t tantivy_jpc(const uint8_t *msg,
                    uintptr_t len,
                    uint8_t *ret,
                    uintptr_t *ret_len);

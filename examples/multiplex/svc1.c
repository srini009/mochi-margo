/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <assert.h>
#include "svc1.h"

static void svc1_do_thing_ult(hg_handle_t handle)
{
    hg_return_t hret;
    svc1_do_thing_out_t out;
    svc1_do_thing_in_t in;
    int ret;
    hg_size_t size;
    void *buffer;
    hg_bulk_t bulk_handle;
    struct hg_info *hgi;
    margo_instance_id mid;

    ret = HG_Get_input(handle, &in);
    assert(ret == HG_SUCCESS);

    printf("Got RPC request with input_val: %d\n", in.input_val);
    out.ret = 0;

    /* set up target buffer for bulk transfer */
    size = 512;
    buffer = calloc(1, 512);
    assert(buffer);

    /* register local target buffer for bulk access */
    hgi = HG_Get_info(handle);
    assert(hgi);
    ret = HG_Bulk_create(hgi->hg_class, 1, &buffer,
        &size, HG_BULK_WRITE_ONLY, &bulk_handle);
    assert(ret == 0);

    mid = margo_hg_class_to_instance(hgi->hg_class);

    /* do bulk transfer from client to server */
    ret = margo_bulk_transfer(mid, HG_BULK_PULL,
        hgi->addr, in.bulk_handle, 0,
        bulk_handle, 0, size);
    assert(ret == 0);

    HG_Free_input(handle, &in);

    hret = HG_Respond(handle, NULL, NULL, &out);
    assert(hret == HG_SUCCESS);

    HG_Bulk_free(bulk_handle);
    HG_Destroy(handle);
    free(buffer);

    return;
}
DEFINE_MARGO_RPC_HANDLER(svc1_do_thing_ult)

static void svc1_do_other_thing_ult(hg_handle_t handle)
{
    hg_return_t hret;
    svc1_do_other_thing_out_t out;
    svc1_do_other_thing_in_t in;
    int ret;
    hg_size_t size;
    void *buffer;
    hg_bulk_t bulk_handle;
    struct hg_info *hgi;
    margo_instance_id mid;

    ret = HG_Get_input(handle, &in);
    assert(ret == HG_SUCCESS);

    printf("Got RPC request with input_val: %d\n", in.input_val);
    out.ret = 0;

    /* set up target buffer for bulk transfer */
    size = 512;
    buffer = calloc(1, 512);
    assert(buffer);

    /* register local target buffer for bulk access */
    hgi = HG_Get_info(handle);
    assert(hgi);
    ret = HG_Bulk_create(hgi->hg_class, 1, &buffer,
        &size, HG_BULK_WRITE_ONLY, &bulk_handle);
    assert(ret == 0);

    mid = margo_hg_class_to_instance(hgi->hg_class);

    /* do bulk transfer from client to server */
    ret = margo_bulk_transfer(mid, HG_BULK_PULL,
        hgi->addr, in.bulk_handle, 0,
        bulk_handle, 0, size);
    assert(ret == 0);

    HG_Free_input(handle, &in);

    hret = HG_Respond(handle, NULL, NULL, &out);
    assert(hret == HG_SUCCESS);

    HG_Bulk_free(bulk_handle);
    HG_Destroy(handle);
    free(buffer);

    return;
}
DEFINE_MARGO_RPC_HANDLER(svc1_do_other_thing_ult)

int svc1_register(margo_instance_id mid, ABT_pool pool, uint32_t mplex_id)
{
    hg_return_t hret;
    hg_id_t id;
    hg_bool_t flag;

    hret = HG_Registered_name(margo_get_class(mid), "svc1_do_thing", &id, &flag);
    if(hret != HG_SUCCESS)
    {
        return(-1);
    }
    
    /* TODO: for each function:
     * - check if registered with mercury or not
     *   - if not, then register
     * - register with margo
     *   - this will put into hash table in mid that can map <id,mplex_id> to
     *     <pool>, checking for duplicate first
     *
     * - elsewhere:
     *   - new variant of DEFINE_MARGO_RPC_HANDLER that:
     *     - looks up registered margo thing
     *     - creates thread targeting pool
     */

    return(-1);
}

void svc1_deregister(margo_instance_id mid, ABT_pool pool, uint32_t mplex_id)
{
    /* TODO: undo what was done in svc1_register() */
    return;
}


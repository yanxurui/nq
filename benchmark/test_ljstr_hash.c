#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#define lj_rol(x, n)  (((x)<<(n)) | ((x)>>(-(int)(n)&(8*sizeof(x)-1))))

static uint32_t lj_getu32(const void *v)
{
    const uint8_t *p = (const uint8_t *)v;
    return (uint32_t)((p[0]<<24) | (p[1]<<16) | (p[2]<<8) | p[3]);
}

// taken from luajit-beta3 lj_str_new
uint32_t compute_hash(const char *str, size_t len)
{
    uint32_t a, b, h = len;

    /* Compute string hash. Constants taken from lookup3 hash by Bob Jenkins. */
    if (len >= 4) {  /* Caveat: unaligned access! */
        a = lj_getu32(str);
        h ^= lj_getu32(str+len-4);
        b = lj_getu32(str+(len>>1)-2);
        h ^= b;
        h -= lj_rol(b, 14);
        b += lj_getu32(str+(len>>2)-1);
    } else if (len > 0) {
        a = *(const uint8_t *)str;
        h ^= *(const uint8_t *)(str+len-1);
        b = *(const uint8_t *)(str+(len>>1));
        h ^= b;
        h -= lj_rol(b, 14);
    }
    a ^= h;
    a -= lj_rol(h, 11);
    b ^= a;
    b -= lj_rol(a, 25);
    h ^= b;
    h -= lj_rol(b, 16);
    return h;
}

// 链表的结点
typedef struct node_s {
    uint32_t v;
    uint32_t c;
    struct node_s *n;
} node_t;

// 插入一个结点，保持升序。如果已经存在不插入，返回0，否则返回1
int insert(node_t *head, uint32_t v)
{
    node_t *p = head;
    while(p->n!=NULL && p->n->v<v)
    {
        p = p->n;
    }
    if(p->n == NULL)
    {
        p->n = (node_t*)malloc(sizeof(node_t));
        p->n->v=v;
        p->n->c=1;
        p->n->n=NULL;
    }
    else
    {
        if(p->n->v==v)
        {
            p->n->c++;
            return 0;
        }
        else
        {
            node_t* temp = (node_t*)malloc(sizeof(node_t));
            temp->v = v;
            p->n->c=1;

            temp->n = p->n;
            p->n = temp;
        }
    }
    return 1;
}

// 输出链表
void output(node_t *head)
{
    node_t *p, *q;
    p=head->n;
    free(head);
    while(p)
    {
        printf("%u\t%d\n", p->v, p->c);
        q=p;
        p=p->n;
        free(q);
    }

}

int main()
{
    uint32_t n, h, count=0;
    char buf[100];

    // head is empty
    node_t *head = (node_t*)malloc(sizeof(node_t));
    head->n=NULL;

    for (int i=0; i<10000; i++)
    {
        n=sprintf(buf, "insert into queue1_rst(m_id, receiver) values(%d, 'receiver1')", i);
        // printf("%s, %d\n", buf, n);
        h = compute_hash(buf, n);
        // printf("%u\n", h);
        if(!insert(head, h))
            count++;
    }
    printf("%d\n", count);
    output(head);
    return 0;
}

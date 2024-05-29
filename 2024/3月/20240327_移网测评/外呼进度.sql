set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;



desc dc_src_rt.crm_marketing_info_new;

select

--     count( if(status != '1', 1, null))                  as `外呼量`,
       count( distinct if(status not in ('1', '5', '10'), phone_no, null)) as `成功量`,
--        concat(round((count( if(status not in ('1', '5', '10'), 1, null)) /
--                      count( if(status != '1', 1, null)) * 100), 2),
--               '%')                                                        as `成功率`,
       task_id
from dc_src_rt.crm_marketing_info_new a
where date_id = '20240326'
  and task_id in
      (
       '51a410832a8a49f7afeff17dfb0692cf'
--        '14906103e3254924b4dd75153b906289',
--        'e9c4d636d09d44ec980d8ee759569ea0',
--        '95c7cb9d6c7140e4b9ad8b6f8c3e0531',
--        '75f08d9768c2436e8120e3e900c3a4c2',
--        'e1d0292d670a466082e97cda521d0a29'
          )
  and a.UPD_TIME > '2024-03-24 05:00:00'
  and a.UPD_TIME < '2024-03-25 00:00:00'
group by task_id;



select count(if(status != '1', 1, null)) as cnt,
       task_id
from dc_src_rt.crm_marketing_info_new a
where date_id = '20240325'
  and a.UPD_TIME > '2024-03-25 05:00:00'
  and a.UPD_TIME < '2024-03-26 00:00:00'
  and task_id in (
                  'b365680e04634dff93201d3324c3cbc8', '1a16de6f02e54ef0adaa51a924dbff4e',
                  'e1d0292d670a466082e97cda521d0a29', 'bd80601f40a345a79a4600c590d1a3fc',
                  '75f08d9768c2436e8120e3e900c3a4c2', '0326f69d3b6049e985c2da67bd188848',
                  '44da9ac254ee4066916484ee6f6606dd', 'f23ff4c6a2ba4c269b00955b3b02ef3b',
                  '60d40651f2124cdbb07e8124e1cbb5cf', '5faa7eb02a4046c4a12f57b0733cc258',
                  '6a2d7cca8e964282b8297822e8c659d6', '1a4ba1154fe64490a5bc884835c5e849',
                  '1e3a0612a977488eac0b408a858c20c6', 'e914720e862745069cf4db4f12de41a9',
                  'bf2b62d945214ffaac08c7827e3817cd', '68d9b26110bd477c8aaaa3d9cdb41b38',
                  '7fcb127f9be74e84a15ef4c217c0d624', '1f2f95ae00c44fea8d8f5e8558da2175',
                  '1c239e77a97545c580c2156332b87ab8', '69933d598caf4146abfc0a8dc25a8903',
                  'c526ef5aead64d4c8c98fe417ef6a48f', '1dd66059c14a414eaa44d3b137e5653e',
                  '9e36c3ae6df64c148746b7e4aab0a127', '1c8b9da20d194e568052a3e40424ab6f',
                  '10125b54753d49e8856f4b2a3e0a6178', '91f935bf98a74d7ba3310ee47f1de1da',
                  '7e756ad5b4284991a8b2c2563dad265d', '78b8a567c14842038d5ce54083b8d320',
                  'c1e5c449d5f44157b8e8afc9387a88de', '9f6ea49d08224ec887e2f48427d27e1f',
                  'b891e41aedf643c1941fd7998f496fd1', '1992732b426b4b868fe8cd814145594b',
                  '7d2009a108074a12b2f4d583c2b28e21', '714eb1128fb54248bf5ff0432163b6cc',
                  '3992dfe7bef548bd9dd6dbaa055706a1', '7dd618ef3d7046baaed658e50bf3d7c3',
                  '6c7e885a124f45a7935be29997d3547b', '8ce45011fc5f42bb97efc8b233bda4b1',
                  '4b26f04e4968492f88be7360a5933478', 'a34624c8ca824304bf0b1a9467d13be6',
                  '5c628fa5b05d4d2db070ada09cf7abb5', 'ab0fca3682a84368a6a13367d4381b4f',
                  '4db7b3e656204b8ea8c23fe53a00329c', '78d99c590d2d479b9e62939ee456675a',
                  '08d792f4c493499d8d55822a0405eba8', 'fd2cab4bb5994a8ea76023b64a2d089c',
                  '58b36e1a05af422c82402d855c6213b5', '1a17a1ea18844d7b8dda9ce59f8033eb',
                  '676ee8ea7b5349519c55a8a8ab34e5ca', '1d86b1893a0d46da856d9c41093614b8',
                  '904e8570cec64dea8adb27279cc53643', '6c468badcee74a05a0140238edbea9c5',
                  '713592180cee4ede8c54b3370019867f', 'f10a2bbdc2224792b1c4a6806670858f',
                  '4b4c08d0c4504132b33751648aacf1ad', '85ba8edba4dd490ba43f53a375c7e66e',
                  '11b27306ca1942f896449b1065e4adcd', 'eaa6e529ecca4858b5b962e287125042',
                  '03dec298f9484a1ea3bdaa163994eee1', '5807353c65784289a24ee18566dee04e',
                  '38d65b61889a4e7eac53b550f91efc91', '1a1e44f0853744da9a2e7d21c3b9eac1',
                  'c6e2aac0fbf145eea5677790f718a880', '1270d6df80874f87b348598311982f7b',
                  '361c9833b87f49eb97953ca3e036d330', '052d989a00a54d24a5725299d1d729ce',
                  '4ffb33aa6c994169a646b1b9c576440b', '8feffa89349a4c598285b4bbc0636d06',
                  'a3193d05092348dea10ee9fe3b3c5b8f', '67a3b386431b4fc5bd5f1a5c27509c0b',
                  '12c14c77ddb2498c851f75a62e214e5d', 'fa1b9ad93b6f49debfa2b0bfc93f9cdd',
                  '4dbebe6b2aa44ee98817999f2300c12d', 'ad7c7e103f4d4349ae4355cc08c7672d',
                  '1c10a5813ed74eb5be1574506315af43', '0dcaa82ed50945c2939ae37507a80590',
                  '93143fa4f98e453fbf71c6ac6a1cad81', 'f00b0b76ed6445c796d7ced89697aa22',
                  '078203796c0443e88f70b0e66d90e0e5', '046f99def4544c9992b168fe276f9b92',
                  '55c39f56009546c8aa9b1239c7e844d3', 'b60c56ce9dd447bb919df1628c77b912',
                  '6ec2d36f50324a2c87435ccaee7cac60', '4ed7c1f633674386a6b2640e69e118ef',
                  '09cb8eb9ebe342488f5607d033412a99', 'b06f939ec55d4023802593843751040d',
                  '4a187d53f10e4364b84c43e01a9c8988', '1ca4d76d0fb44f919eb537acbb08f463',
                  'd7ba707f88b446abbea5bdb96dde12a8', '44368b431d1a40248eccaaf8e9b20ace',
                  'df1c85c2c5a34e63b371975761143adf', '4fb9777dd06e4fe7b3764ad12033c645',
                  '9a00a2633f4b4bd28eed0ab6d6b3b410', '8e2d783f02c441f78d84f4d70e26c882',
                  'a48155e8be574d69afdcb41c48b509ea', 'ae97f9e4207348ae8d5f9bb4045e2c7c',
                  '2d86b294f1d34f69bc42b333f5264aa5', '9ec5fea599c6452680a5321afe01cabd',
                  'b2baf774b1d14c06a8fa53a8ff15a584', '173e29d4710a4317a0130b5f1db28796',
                  '4482f628069544b7935dcafe02ea29d9', '93b2fcdf71864c9dbf37c1ffa55eff37',
                  'fc61c7e421864de690d48055c396bc17', '41e234c2d9ae4b1091b3e64a70b7a54d',
                  '800f495573cf4374a2fd835ede82f9d2', 'eec2b0db214e4261a2e3828b5e00dc67',
                  '7e734e654efa433c81bf3bf43b7a5a6c', 'c6f0796a21a24bf794e1c42e2d56a715',
                  '47429a23380d45b087f1e3eb14e8e9e2', '07cf5db897474e619fae11102eeb96ce',
                  'b6b035d6d00b41739624fa79cafcb2ff', 'ec7362de9cee4738ac8a241e7857ddbf',
                  '14906103e3254924b4dd75153b906289', 'e9c4d636d09d44ec980d8ee759569ea0',
                  '95c7cb9d6c7140e4b9ad8b6f8c3e0531'
    )
group by task_id;

